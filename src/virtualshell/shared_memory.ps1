$ErrorActionPreference = 'Stop'

$script:VSNativeInitialized   = $false
$script:VSNativeDllPath       = $null
$script:VSNativePreferredPath = $null

$script:VS_STATUS_OK           = 0
$script:VS_STATUS_TIMEOUT      = 1
$script:VS_STATUS_WOULD_BLOCK  = 2
$script:VS_STATUS_SMALL_BUFFER = 3
$script:VS_STATUS_INVALID_ARG  = -1
$script:VS_STATUS_SYS_ERROR    = -2
$script:VS_STATUS_BAD_STATE    = -3

function Initialize-VSSharedMemoryNative {
    param(
        # Kan være EXAKT fil ("C:\...\win_pwsh.dll") ELLER en mappe-hint (da søkes det etter win_pwsh.dll der)
        [string]$PreferredPath
    )

    if (-not [string]::IsNullOrWhiteSpace($PreferredPath)) {
        $script:VSNativePreferredPath = $PreferredPath
    }

    if (-not $script:VSNativePreferredPath -and $env:VIRTUALSHELL_WIN_PWSH_DLL) {
        $script:VSNativePreferredPath = $env:VIRTUALSHELL_WIN_PWSH_DLL
    }

    if ($script:VSNativeInitialized) {
        if (-not [string]::IsNullOrWhiteSpace($PreferredPath) -and $script:VSNativeDllPath) {
            try {
                $resolvedHint = (Resolve-Path -LiteralPath $PreferredPath -ErrorAction Stop).Path
                if ($resolvedHint -ne $script:VSNativeDllPath -and
                    (Split-Path -Leaf $resolvedHint) -ne (Split-Path -Leaf $script:VSNativeDllPath)) {
                    Write-Verbose "win_pwsh.dll already loaded from '$($script:VSNativeDllPath)'; ignoring hint '$resolvedHint'."
                }
            } catch { }
        }
        return
    }

    # --- Bygg kandidat-liste ---
    $candidatePaths = [System.Collections.Generic.List[string]]::new()

    if (-not [string]::IsNullOrWhiteSpace($script:VSNativePreferredPath)) {
        # Hvis hint er en mappe: legg til mappe\win_pwsh.dll
        if (Test-Path -LiteralPath $script:VSNativePreferredPath -PathType Container) {
            $candidatePaths.Add( (Join-Path $script:VSNativePreferredPath 'win_pwsh.dll') )
        } else {
            $candidatePaths.Add($script:VSNativePreferredPath) # antatt fil
        }
    }

    $relativeCandidates = @(
        'win_pwsh.dll',
        '..\win_pwsh.dll',
        '..\..\win_pwsh.dll',
        '..\..\build\win_pwsh_dll\Release\win_pwsh.dll',
        '..\..\build\win_pwsh_dll\Debug\win_pwsh.dll',
        '..\..\build\win_pwsh_dll\RelWithDebInfo\win_pwsh.dll',
        '..\..\build\win_pwsh_dll\MinSizeRel\win_pwsh.dll',
        '..\..\win_pwsh_dll\win_pwsh.dll',
        '..\..\win_pwsh_dll\bin\win_pwsh.dll'
    )
    foreach ($rel in $relativeCandidates) {
        $candidatePaths.Add( (Join-Path $PSScriptRoot $rel) )
    }

    # --- Last inn DLL + gjør den søkbar for DllImport ---
    $loadedPath = $null
    $loadErrors = @()
    foreach ($candidate in $candidatePaths) {
        if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
        try {
            if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { 
                $loadErrors += "Not found: $candidate"
                continue 
            }
            $resolved = (Resolve-Path -LiteralPath $candidate -ErrorAction Stop).Path

            # 1) Sørg for at katalogen ligger først i PATH (så DllImport("win_pwsh.dll") finner den)
            $dllDir = Split-Path -Parent $resolved
            if ($dllDir -and (Test-Path -LiteralPath $dllDir -PathType Container)) {
                $oldPath = [System.Environment]::GetEnvironmentVariable("PATH", "Process")
                if ([string]::IsNullOrEmpty($oldPath)) {
                    [System.Environment]::SetEnvironmentVariable("PATH", $dllDir, "Process")
                } else {
                    # Unngå duplikater
                    $pathParts = $oldPath -split ';' | Where-Object { $_ -ne $dllDir -and $_ -ne '' }
                    $newPath = ($dllDir + ';' + ($pathParts -join ';'))
                    [System.Environment]::SetEnvironmentVariable("PATH", $newPath, "Process")
                }
            }

            # 2) Explicit load (pins the module in the process)
            # Use NativeLibrary.Load if available (.NET Core/5+), otherwise just rely on PATH
            try {
                $null = [System.Runtime.InteropServices.NativeLibrary]::Load($resolved)
            } catch {
                # PowerShell 5.1 doesn't have NativeLibrary - that's OK, DllImport will find it via PATH
            }

            $loadedPath = $resolved
            break
        } catch {
            $loadErrors += "Failed to load $candidate : $_"
            continue
        }
    }

    if (-not $loadedPath) {
        $errorMsg = "Unable to locate win_pwsh.dll. Provide -PreferredPath (file or folder) or set VIRTUALSHELL_WIN_PWSH_DLL."
        $errorMsg += "`n`nTried paths:`n" + ($loadErrors -join "`n")
        throw $errorMsg
    }

    $script:VSNativeDllPath = $loadedPath

    if (-not ('VS.Native.ShmNative' -as [type])) {
        try {
            Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

namespace VS.Native {
    public enum VS_Status : int {
        OK = 0,
        TIMEOUT = 1,
        WOULD_BLOCK = 2,
        SMALL_BUFFER = 3,
        INVALID_ARG = -1,
        SYS_ERROR = -2,
        BAD_STATE = -3
    }

    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    public struct VS_Header {
        public UInt32 magic;
        public UInt32 version;
        public UInt64 frame_bytes;
        public UInt64 python_seq;
        public UInt64 powershell_seq;
        public UInt64 python_length;
        public UInt64 powershell_length;
        
        // Offset-based zero-copy fields
        public UInt64 ps_data_offset;
        public UInt64 ps_data_length;
        public UInt64 ps_data_seq;
        public UInt32 ps_data_valid;
        
        // Chunked transfer fields (always-on chunking)
        public UInt32 ps_chunk_index;
        public UInt64 ps_total_size;
        public UInt64 ps_chunk_size;
        public UInt64 ps_num_chunks;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 3)]
        public UInt64[] reserved;

        public UInt64 MagicAndVersion {
            get { return ((UInt64)version << 32) | magic; }
        }
    }

    public static class ShmNative {
        // Viktig: navnet må matche modulens eksportnavn (ikke full path).
        private const string DllName = "win_pwsh.dll";

        [DllImport(DllName, CharSet = CharSet.Unicode, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr VS_OpenChannel(string name, UInt64 frameBytes, UInt32 numSlots, Int32 useGlobalFallback);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void VS_CloseChannel(IntPtr channel);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WritePs2Py(IntPtr channel, byte[] data, UInt64 len, UInt32 timeoutMs, out UInt64 nextSeq);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_ReadPy2Ps(IntPtr channel, byte[] dst, UInt64 capacity, out UInt64 outLen, UInt32 timeoutMs);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_GetHeader(IntPtr channel, out VS_Header header);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WritePy2Ps(IntPtr channel, byte[] data, UInt64 len, UInt32 timeoutMs, out UInt64 nextSeq);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_ReadPs2Py(IntPtr channel, byte[] dst, UInt64 capacity, out UInt64 outLen, UInt32 timeoutMs);

        // Fast object serialization (C++/CLI)
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WriteObjectPs2Py(IntPtr channel, IntPtr objPtr, UInt32 timeoutMs, out UInt64 nextSeq, out UInt64 serializedLen);
        
        // Test GCHandle marshaling
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
        public static extern Int32 TestGCHandleMarshaling(IntPtr objPtr, [MarshalAs(UnmanagedType.LPWStr)] System.Text.StringBuilder outTypeName, Int32 typeNameLen);

        // Offset-based zero-copy API
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_SetDataOffset(IntPtr channel, UInt64 byteOffset, UInt64 byteLength);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_GetDataOffset(IntPtr channel, out UInt64 outOffset, out UInt64 outLen, out UInt64 outSeq);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_AckDataOffset(IntPtr channel);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WaitForDataOffsetAck(IntPtr channel, UInt32 timeoutMs);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_ClearDataOffset(IntPtr channel);
        
        // Get base pointer of shared memory region (needed to calculate offsets)
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr VS_GetSharedMemoryBase(IntPtr channel);
        
        // Chunked transfer API (always-on chunking)
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_BeginChunkedTransfer(IntPtr channel, UInt64 totalSize, UInt64 chunkSize);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_SendChunk(IntPtr channel, UInt32 chunkIndex, byte[] data, UInt64 length, UInt32 timeoutMs);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WaitForChunk(IntPtr channel, out UInt32 outChunkIndex, out UInt64 outOffset, out UInt64 outLength, UInt32 timeoutMs);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_AckChunk(IntPtr channel);
    }
}
"@ | Out-Null
        } catch {
            Write-Error "Failed to add VS.Native types: $_"
            Write-Error "Stack: $($_.ScriptStackTrace)"
            throw
        }
    }

    $script:VSNativeInitialized = $true
}

class SharedMemoryChannel {
    [IntPtr] $Handle
    [UInt64] $FrameBytes
    [string] $Name
    [byte[]] $ReadScratch

    SharedMemoryChannel([string]$Name, [int]$FrameBytes, [int]$NumSlots = 1) {
        if ([string]::IsNullOrWhiteSpace($Name)) {
            throw "Shared memory channel name cannot be empty."
        }
        if ($FrameBytes -le 0) {
            throw "FrameBytes must be positive."
        }
        if ($NumSlots -ne 1) {
            throw "SharedMemoryChannel supports NumSlots = 1 (requested $NumSlots)."
        }

        Initialize-VSSharedMemoryNative

        $frame = [uint64]$FrameBytes
        $native = $this.GetNativeInteropType()
        Write-Host "[DEBUG] PowerShell opening channel: Name='$Name', FrameBytes=$frame"
        $nativeHandle = $native::VS_OpenChannel($Name, $frame, [uint32]$NumSlots, 1)
        if ($nativeHandle -eq [IntPtr]::Zero) {
            throw "VS_OpenChannel failed for '$Name' (frame=$FrameBytes)."
        }
        Write-Host "[DEBUG] PowerShell got handle: $nativeHandle"
                $this.Name       = $Name
        $this.FrameBytes = $frame
        $this.Handle     = $nativeHandle
        $this.ReadScratch = [byte[]]::new($FrameBytes)
    }

    hidden [void] EnsureOpen() {
        if ($this.Handle -eq [IntPtr]::Zero) {
            throw "SharedMemoryChannel '$($this.Name)' is closed."
        }
    }

    hidden [Type] GetNativeInteropType() {
        $type = 'VS.Native.ShmNative' -as [type]
        if (-not $type) {
            Initialize-VSSharedMemoryNative
            $type = 'VS.Native.ShmNative' -as [type]
            if (-not $type) {
                throw "Failed to load VS.Native.ShmNative type even after initialization"
            }
        }
        return $type
    }

    hidden [object] GetHeader() {
        $this.EnsureOpen()
        $native = $this.GetNativeInteropType()
        $hdr = New-Object -TypeName 'VS.Native.VS_Header'
        if (-not $hdr.reserved) {
            $hdr.reserved = [UInt64[]]::new(6)
        }
        $status = $native::VS_GetHeader($this.Handle, [ref]$hdr)
        if ($status -ne $script:VS_STATUS_OK) {
            throw "VS_GetHeader failed for '$($this.Name)' with status $status."
        }
        return $hdr
    }

    [UInt64] GetPythonSeq()        { return ($this.GetHeader()).python_seq }
    [UInt64] GetPowerShellSeq()    { return ($this.GetHeader()).powershell_seq }
    [UInt64] GetPythonLength()     { return ($this.GetHeader()).python_length }
    [UInt64] GetPowerShellLength() { return ($this.GetHeader()).powershell_length }

    [byte[]] ReadFromPython([int]$TimeoutMs = 5000) {
        $this.EnsureOpen()

        $buffer = $this.ReadScratch
        # NOTE: PowerShell [int] is Int32 with max ~2GB
        # For frames > 2GB, we need to use [long] or handle carefully
        $frameSize = [long]$this.FrameBytes
        
        if (-not $buffer -or $buffer.Length -ne $frameSize) {
            # Check if frame size exceeds Int32.MaxValue (~2GB)
            if ($frameSize -gt [int]::MaxValue) {
                throw "Frame size $($frameSize/1MB) MB exceeds PowerShell array limit of ~2GB (Int32.MaxValue)"
            }
            $buffer = [byte[]]::new([int]$frameSize)
            $this.ReadScratch = $buffer
        }

        $native = $this.GetNativeInteropType()
        $outLen = [UInt64]0
        $status = $native::VS_ReadPy2Ps(
            $this.Handle,
            $buffer,
            [UInt64]$buffer.Length,
            [ref]$outLen,
            [UInt32][Math]::Max(0, $TimeoutMs)
        )

        switch ($status) {
            ($script:VS_STATUS_OK) { }
            ($script:VS_STATUS_TIMEOUT)      { throw "Timed out waiting for Python data on '$($this.Name)'." }
            ($script:VS_STATUS_SMALL_BUFFER) { throw "Python payload length $outLen exceeds frame capacity $([UInt64]$buffer.Length) on '$($this.Name)'." }
            default { throw "VS_ReadPy2Ps failed with status $status on '$($this.Name)'." }
        }

        $length = [int]$outLen
        if ($length -le 0) { return [byte[]]::new(0) }

        $result = [byte[]]::new($length)
        [Buffer]::BlockCopy($buffer, 0, $result, 0, $length)
        return $result
    }

    [UInt64] WriteToPython([byte[]]$Data, [int]$TimeoutMs = 5000) {
        $this.EnsureOpen()
        $payload = if ($null -eq $Data) { [byte[]]::new(0) } else { $Data }
        $length = [UInt64]$payload.Length
        if ($length -gt $this.FrameBytes) {
            throw "Payload length $length exceeds frame capacity $($this.FrameBytes) on '$($this.Name)'."
        }

        $native = $this.GetNativeInteropType()
        $nextSeq = [UInt64]0
        $status = $native::VS_WritePs2Py(
            $this.Handle,
            $payload,
            $length,
            [UInt32][Math]::Max(0, $TimeoutMs),
            [ref]$nextSeq
        )

        if ($status -ne $script:VS_STATUS_OK) {
            throw "VS_WritePs2Py failed with status $status on '$($this.Name)'."
        }

        return $nextSeq
    }

    # Fast object serialization via C++/CLI (10-100x faster than PowerShell)
    [UInt64] WriteObjectToPython([object]$Object, [int]$TimeoutMs = 5000) {
        $this.EnsureOpen()
        
        # Debug: Check object type
        Write-Host "[WriteObjectToPython] Object type: $($Object.GetType().FullName), Length: $(if ($Object.PSObject.Properties['Length']) { $Object.Length } else { 'N/A' })"
        
        # Use GCHandle instead of COM marshaling (works when DLL loaded in same AppDomain via Add-Type)
        # GCHandle gives C++/CLI direct access to the actual object, not a __ComObject wrapper
        $gcHandle = [System.Runtime.InteropServices.GCHandle]::Alloc($Object)
        
        try {
            $handlePtr = [System.Runtime.InteropServices.GCHandle]::ToIntPtr($gcHandle)
            
            Write-Host "[WriteObjectToPython] GCHandle IntPtr: $handlePtr"
            
            $native = $this.GetNativeInteropType()
            $nextSeq = [UInt64]0
            $serializedLen = [UInt64]0
            
            $status = $native::VS_WriteObjectPs2Py(
                $this.Handle,
                $handlePtr,
                [UInt32][Math]::Max(0, $TimeoutMs),
                [ref]$nextSeq,
                [ref]$serializedLen
            )
            
            Write-Host "[WriteObjectToPython] Status: $status, SerializedLen: $serializedLen, NextSeq: $nextSeq"
            
            if ($status -ne $script:VS_STATUS_OK) {
                throw "VS_WriteObjectPs2Py failed with status $status on '$($this.Name)'."
            }
            
            return $nextSeq
        }
        finally {
            # Free the GCHandle
            $gcHandle.Free()
        }
    }

    # Zero-copy offset-based transfer to Python (NEW - PREFERRED METHOD)
    # This is true zero-copy: PowerShell copies to shared memory once, Python reads directly
    [void] WriteArrayViaOffset([byte[]]$Array, [int]$TimeoutMs = 30000) {
        $this.EnsureOpen()
        
        if ($null -eq $Array -or $Array.Length -eq 0) {
            throw "Array cannot be null or empty for offset-based transfer"
        }
        
        $byteLength = [UInt64]$Array.Length
        
        if ($byteLength -gt $this.FrameBytes) {
            throw "Array length $byteLength exceeds frame capacity $($this.FrameBytes)"
        }
        
        Write-Verbose "[WriteArrayViaOffset] Transferring $byteLength bytes via offset-based zero-copy"
        
        $native = $this.GetNativeInteropType()
        
        # Get base address of shared memory
        $shmBase = $native::VS_GetSharedMemoryBase($this.Handle)
        if ($shmBase -eq [IntPtr]::Zero) {
            throw "Failed to get shared memory base address"
        }
        
        # Calculate offset to PS→Python buffer
        # Layout: Header (128 bytes) + PY→PS buffer (frame_bytes) + PS→PY buffer (frame_bytes)
        $headerSize = 128
        $ps2pyOffset = $headerSize + $this.FrameBytes
        
        # Get pointer to PS→Python buffer in shared memory
        $ps2pyPtr = [IntPtr]::Add($shmBase, [int]$ps2pyOffset)
        
        # Pin the array so GC won't move it during copy
        $gcHandle = [System.Runtime.InteropServices.GCHandle]::Alloc($Array, [System.Runtime.InteropServices.GCHandleType]::Pinned)
        
        try {
            # Copy data from array to shared memory PS→Python buffer
            # This is the ONE and ONLY memcpy - Python will read directly from shared memory
            # Marshal.Copy signature: Copy(byte[] source, int startIndex, IntPtr destination, int length)
            [System.Runtime.InteropServices.Marshal]::Copy($Array, 0, $ps2pyPtr, [int]$byteLength)
            
            Write-Verbose "[WriteArrayViaOffset] Copied $byteLength bytes to shared memory at offset $ps2pyOffset"
            
            # Set offset metadata to tell Python where the data is
            Write-Host "[DEBUG] Calling VS_SetDataOffset with offset=$ps2pyOffset, length=$byteLength"
            Write-Host "[DEBUG] Shared memory base: $shmBase"
            Write-Host "[DEBUG] Channel handle: $($this.Handle)"
            $status = $native::VS_SetDataOffset($this.Handle, $ps2pyOffset, $byteLength)
            Write-Host "[DEBUG] VS_SetDataOffset returned status=$status"
            
            # Read back ps_data_valid directly to verify
            $ps_data_valid_ptr = [IntPtr]::Add($shmBase, 72)
            $ps_data_valid_value = [System.Runtime.InteropServices.Marshal]::ReadInt32($ps_data_valid_ptr)
            Write-Host "[DEBUG] ps_data_valid after set: $ps_data_valid_value"
            
            if ($status -ne $script:VS_STATUS_OK) {
                throw "VS_SetDataOffset failed with status $status"
            }
            
            Write-Verbose "[WriteArrayViaOffset] Waiting for Python ACK (timeout: $TimeoutMs ms)..."
            
            # Wait for Python to acknowledge it's done reading
            $status = $native::VS_WaitForDataOffsetAck($this.Handle, [UInt32]$TimeoutMs)
            
            if ($status -eq 1) {  # VS_STATUS_TIMEOUT
                throw "Timeout waiting for Python to acknowledge offset read (${TimeoutMs}ms)"
            }
            elseif ($status -ne $script:VS_STATUS_OK) {
                throw "VS_WaitForDataOffsetAck failed with status $status"
            }
            
            Write-Verbose "[WriteArrayViaOffset] OK - Python acknowledged - transfer complete"
        }
        finally {
            # Always unpin the array when done
            $gcHandle.Free()
            
            # Clear offset metadata
            [void]$native::VS_ClearDataOffset($this.Handle)
        }
    }
    
    # Fire-and-forget version - does NOT wait for Python ACK
    # Used by SendBytesNoWait where Python will manage its own lifecycle
    [void] WriteArrayViaOffsetNoWait([byte[]]$Array) {
        $this.EnsureOpen()
        
        if ($null -eq $Array -or $Array.Length -eq 0) {
            throw "Array cannot be null or empty"
        }
        
        $byteLength = $Array.Length
        $native = $this.GetNativeInteropType()
        
        # Pin the byte array so it doesn't move in memory
        $gcHandle = [System.Runtime.InteropServices.GCHandle]::Alloc($Array, [System.Runtime.InteropServices.GCHandleType]::Pinned)
        
        try {
            # Get base address of shared memory
            $base = $native::VS_GetSharedMemoryBase($this.Handle)
            if ($base -eq [IntPtr]::Zero) {
                throw "Failed to get shared memory base address"
            }
            
            # Calculate offset for PS→PY buffer (header + frame_bytes)
            $ps2pyOffset = 128 + $this.FrameBytes
            $destPtr = [IntPtr]::Add($base, $ps2pyOffset)
            
            # Copy data to shared memory
            [System.Runtime.InteropServices.Marshal]::Copy($Array, 0, $destPtr, $byteLength)
            
            Write-Verbose "[WriteArrayViaOffsetNoWait] Copied $byteLength bytes, signaling Python (NO WAIT)"
            
            # Signal Python that data is ready (this sets event)
            $status = $native::VS_SetDataOffset($this.Handle, [UInt64]$ps2pyOffset, [UInt64]$byteLength)
            
            if ($status -ne $script:VS_STATUS_OK) {
                throw "VS_SetDataOffset failed with status $status"
            }
            
            Write-Verbose "[WriteArrayViaOffsetNoWait] Data signaled, returning immediately"
            # Do NOT wait for ACK - Python will read when ready
        }
        finally {
            # Always unpin the array
            $gcHandle.Free()
        }
        # Note: Do NOT clear offset - Python still needs to read it!
    }

    # Zero-copy pinned pointer transfer to Python (OLD METHOD - DEPRECATED)
    # This method doesn't work across processes due to address space isolation
    # Use WriteArrayViaOffset instead
    [void] WritePinnedArrayToPython([byte[]]$Array, [int]$TimeoutMs = 30000) {
        $this.EnsureOpen()
        
        if ($null -eq $Array -or $Array.Length -eq 0) {
            throw "Array cannot be null or empty for pinned transfer"
        }
        
        # Pin the array so GC won't move it
        $gcHandle = [System.Runtime.InteropServices.GCHandle]::Alloc($Array, [System.Runtime.InteropServices.GCHandleType]::Pinned)
        
        try {
            # Get pointer to pinned array
            $pinnedPtr = $gcHandle.AddrOfPinnedObject()
            $ptrAddress = [UInt64]$pinnedPtr.ToInt64()
            $byteLength = [UInt64]$Array.Length
            
            Write-Host "[WritePinnedArrayToPython] Pinned $byteLength bytes at 0x$($ptrAddress.ToString('X'))"
            
            # Tell Python about the pointer
            $native = $this.GetNativeInteropType()
            $status = $native::VS_SetPinnedPointer($this.Handle, $ptrAddress, $byteLength)
            
            if ($status -ne $script:VS_STATUS_OK) {
                throw "VS_SetPinnedPointer failed with status $status"
            }
            
            Write-Host "[WritePinnedArrayToPython] Waiting for Python ACK (timeout: $TimeoutMs ms)..."
            
            # Wait for Python to acknowledge it's done reading
            $status = $native::VS_WaitForPinnedPointerAck($this.Handle, [UInt32]$TimeoutMs)
            
            if ($status -eq 1) {  # VS_STATUS_TIMEOUT
                throw "Timeout waiting for Python to acknowledge pinned pointer read (${TimeoutMs}ms)"
            }
            elseif ($status -ne $script:VS_STATUS_OK) {
                throw "VS_WaitForPinnedPointerAck failed with status $status"
            }
            
            Write-Host "[WritePinnedArrayToPython] OK - Python acknowledged - transfer complete"
        }
        finally {
            # Always unpin the array when done
            $gcHandle.Free()
            
            # Clear pointer metadata
            $native = $this.GetNativeInteropType()
            [void]$native::VS_ClearPinnedPointer($this.Handle)
        }
    }

    [void] Dispose() {
        if ($this.Handle -ne [IntPtr]::Zero) {
            $native = $this.GetNativeInteropType()
            $native::VS_CloseChannel($this.Handle)
            $this.Handle = [IntPtr]::Zero
        }
    }

    [void] Close() { $this.Dispose() }
}
