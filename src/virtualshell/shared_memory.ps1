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
    foreach ($candidate in $candidatePaths) {
        if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
        try {
            if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
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
            [void][System.Runtime.InteropServices.NativeLibrary]::Load($resolved)

            $loadedPath = $resolved
            break
        } catch {
            continue
        }
    }

    if (-not $loadedPath) {
        throw "Unable to locate win_pwsh.dll. Provide -PreferredPath (file or folder) or set VIRTUALSHELL_WIN_PWSH_DLL."
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

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 10)]
        public UInt64[] reserved;

        public UInt64 MagicAndVersion => ((UInt64)version << 32) | magic;
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
        $nativeHandle = $native::VS_OpenChannel($Name, $frame, [uint32]$NumSlots, 1)
        if ($nativeHandle -eq [IntPtr]::Zero) {
            throw "VS_OpenChannel failed for '$Name' (frame=$FrameBytes)."
        }

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
            $hdr.reserved = [UInt64[]]::new(10)
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
        if (-not $buffer -or $buffer.Length -ne [int]$this.FrameBytes) {
            $buffer = [byte[]]::new([int]$this.FrameBytes)
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

    [void] Dispose() {
        if ($this.Handle -ne [IntPtr]::Zero) {
            $native = $this.GetNativeInteropType()
            $native::VS_CloseChannel($this.Handle)
            $this.Handle = [IntPtr]::Zero
        }
    }

    [void] Close() { $this.Dispose() }
}
