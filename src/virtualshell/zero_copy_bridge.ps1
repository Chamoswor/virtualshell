# Zero-Copy Bridge v2 - PowerShell side
# Clean API for sending/receiving data to/from Python

$ErrorActionPreference = 'Stop'

# =============================================================================
# DLL LOADING
# =============================================================================

$script:VSNativeInitialized = $false
$script:VSNativeDllPath = $null

function Initialize-VSNative {
    param([string]$PreferredPath)
    
    if ($script:VSNativeInitialized) {
        return
    }
    
    # Find DLL
    $candidates = @()
    
    if ($PreferredPath) {
        if (Test-Path -LiteralPath $PreferredPath -PathType Leaf) {
            $candidates += $PreferredPath
        } elseif (Test-Path -LiteralPath $PreferredPath -PathType Container) {
            $candidates += (Join-Path $PreferredPath 'win_pwsh.dll')
        }
    }
    
    if ($env:VIRTUALSHELL_WIN_PWSH_DLL) {
        $candidates += $env:VIRTUALSHELL_WIN_PWSH_DLL
    }
    
    $relativePaths = @(
        'win_pwsh.dll',
        '..\win_pwsh.dll',
        '..\..\build\win_pwsh_dll\Release\win_pwsh.dll',
        '..\..\build\win_pwsh_dll\Debug\win_pwsh.dll'
    )
    
    foreach ($rel in $relativePaths) {
        $candidates += (Join-Path $PSScriptRoot $rel)
    }
    
    $loadedPath = $null
    foreach ($candidate in $candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
        if (-not (Test-Path -LiteralPath $candidate)) { continue }
        
        try {
            $fullPath = Resolve-Path -LiteralPath $candidate
            [System.Reflection.Assembly]::LoadFile($fullPath.Path) | Out-Null
            $loadedPath = $fullPath.Path
            break
        } catch {
            continue
        }
    }
    
    if (-not $loadedPath) {
        throw "Unable to locate win_pwsh.dll. Set VIRTUALSHELL_WIN_PWSH_DLL or provide -PreferredPath"
    }
    
    $script:VSNativeDllPath = $loadedPath
    
    # Define P/Invoke signatures with full DLL path
    if (-not ('VS.Native.V2' -as [type])) {
        $typeDefinition = @"
using System;
using System.Runtime.InteropServices;

namespace VS.Native {
    public static class V2 {
        private const string DllName = @"$loadedPath";
        
        // Channel lifecycle
        [DllImport(DllName, CharSet = CharSet.Unicode, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr VS_CreateChannel(string name, UInt64 frameBytes);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void VS_DestroyChannel(IntPtr handle);
        
        // PowerShell → Python
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_BeginPs2PyTransfer(IntPtr handle, UInt64 totalSize, UInt64 chunkSize);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_SendPs2PyChunk(IntPtr handle, UInt32 chunkIndex, byte[] data, UInt64 length, UInt32 timeoutMs);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WaitPs2PyAck(IntPtr handle, UInt32 timeoutMs);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_FinishPs2PyTransfer(IntPtr handle);
        
        // Python → PowerShell
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_WaitPy2PsChunk(IntPtr handle, out UInt32 chunkIndex, out UInt64 offset, out UInt64 length, UInt32 timeoutMs);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_AckPy2PsChunk(IntPtr handle);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_IsPy2PsComplete(IntPtr handle);
        
        // Utility
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr VS_GetMemoryBase(IntPtr handle);
        
        // Object serialization
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_SendObjectPs2Py(IntPtr handle, IntPtr gcHandle, UInt64 chunkSize, UInt32 timeoutMs);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern Int32 VS_SerializeObject(IntPtr gcHandle, out IntPtr outBytes, out UInt64 outLength);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void VS_FreeBytes(IntPtr bytes);
    }
}
"@
        Add-Type -TypeDefinition $typeDefinition
    }
    
    $script:VSNativeInitialized = $true
}

# =============================================================================
# STATUS CODES
# =============================================================================

$script:VS_OK = 0
$script:VS_TIMEOUT = 1
$script:VS_WOULD_BLOCK = 2
$script:VS_ERR_INVALID = -1
$script:VS_ERR_SYSTEM = -2
$script:VS_ERR_BAD_STATE = -3
$script:VS_ERR_TOO_LARGE = -4

# =============================================================================
# POWERSHELL API
# =============================================================================

function Send-VariableToPython {
    <#
    .SYNOPSIS
        Send PowerShell variable to Python (zero-copy after serialization)
    
    .PARAMETER ChannelName
        Channel name (without Local\ or Global\ prefix)
    
    .PARAMETER Variable
        Variable to send (with or without $)
    
    .PARAMETER ChunkSizeMB
        Chunk size in MB (default: 4)
    
    .PARAMETER FrameSizeMB
        Frame size in MB (default: 64)
    
    .PARAMETER TimeoutSeconds
        Timeout in seconds (default: 30)
    
    .EXAMPLE
        $data = 1..1000
        Send-VariableToPython -ChannelName "my_channel" -Variable $data
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ChannelName,
        
        [Parameter(Mandatory)]
        [object]$Variable,
        
    [int]$ChunkSizeMB = 4,
    [int]$FrameSizeMB = 64,
    [int]$TimeoutSeconds = 30,
    [ValidateSet('Local','Global')]
    [string]$Scope = 'Local'
    )
    
    Initialize-VSNative
    
    $native = [VS.Native.V2]
    $chunkBytes = [UInt64]$ChunkSizeMB * 1024 * 1024
    $frameBytes = [UInt64]$FrameSizeMB * 1024 * 1024
    $timeoutMs = [UInt32]($TimeoutSeconds * 1000)
    
    # Create channel name (use provided scope unless already qualified)
    if ($ChannelName -like '*\*') {
        $fullChannelName = $ChannelName
    } else {
        $fullChannelName = "$Scope\$ChannelName"
    }
    
    # Open channel (Python should have created it)
    $handle = $native::VS_CreateChannel($fullChannelName, $frameBytes)
    if ($handle -eq [IntPtr]::Zero) {
        throw "Failed to open channel: $fullChannelName"
    }
    
    Write-Host "Channel opened: $handle" -ForegroundColor Cyan
    
    try {
        # Allocate GCHandle (normal, not pinned - C++/CLI will handle the object)
        $gcHandle = [System.Runtime.InteropServices.GCHandle]::Alloc($Variable)
        
        Write-Host "GCHandle created, calling VS_SendObjectPs2Py..." -ForegroundColor Cyan
        
        try {
            # Use fast C++/CLI serialization and chunked send
            # Pass GCHandle as IntPtr (not AddrOfPinnedObject)
            $gcHandleIntPtr = [System.Runtime.InteropServices.GCHandle]::ToIntPtr($gcHandle)
            $result = $native::VS_SendObjectPs2Py($handle, $gcHandleIntPtr, $chunkBytes, $timeoutMs)
            
            if ($result -ne $script:VS_OK) {
                throw "VS_SendObjectPs2Py failed with status: $result"
            }
        }
        finally {
            $gcHandle.Free()
        }
    }
    finally {
        # PowerShell does NOT destroy channel - Python owns it
    }
}

function Receive-VariableFromPython {
    <#
    .SYNOPSIS
        Receive data from Python into PowerShell variable (zero-copy)
    
    .PARAMETER ChannelName
        Channel name (without Local\ or Global\ prefix)
    
    .PARAMETER VariableName
        Variable name to store result (without $)
    
    .PARAMETER FrameSizeMB
        Frame size in MB (default: 64)
    
    .PARAMETER TimeoutSeconds
        Timeout in seconds (default: 30)
    
    .EXAMPLE
        Receive-VariableFromPython -ChannelName "my_channel" -VariableName "mydata"
        # Now $mydata contains the received bytes
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ChannelName,
        
        [Parameter(Mandatory)]
        [string]$VariableName,
        
    [int]$FrameSizeMB = 64,
    [int]$TimeoutSeconds = 30,
    [ValidateSet('Local','Global')]
    [string]$Scope = 'Local'
    )
    
    Initialize-VSNative
    
    $native = [VS.Native.V2]
    $frameBytes = [UInt64]$FrameSizeMB * 1024 * 1024
    $timeoutMs = [UInt32]($TimeoutSeconds * 1000)
    $completionPollMs = [int][Math]::Min(200, [Math]::Max(10, $TimeoutSeconds * 50))
    
    # Create channel name (use provided scope unless already qualified)
    if ($ChannelName -like '*\*') {
        $fullChannelName = $ChannelName
    } else {
        $fullChannelName = "$Scope\$ChannelName"
    }
    
    # Open channel (Python should have created it)
    $handle = $native::VS_CreateChannel($fullChannelName, $frameBytes)
    if ($handle -eq [IntPtr]::Zero) {
        throw "Failed to open channel: $fullChannelName"
    }
    
    try {
        # Get shared memory base
        $memBase = $native::VS_GetMemoryBase($handle)
        if ($memBase -eq [IntPtr]::Zero) {
            throw "Failed to get shared memory base"
        }
        
        $chunks = [System.Collections.Generic.List[byte[]]]::new()
        $totalSize = 0
        
        while ($true) {
            # Wait for chunk
            $chunkIndex = [UInt32]0
            $offset = [UInt64]0
            $length = [UInt64]0
            
            $result = $native::VS_WaitPy2PsChunk($handle, [ref]$chunkIndex, [ref]$offset, [ref]$length, $timeoutMs)
            
            if ($result -eq $script:VS_TIMEOUT) {
                if ($native::VS_IsPy2PsComplete($handle) -eq 1) {
                    break
                }
                throw "Timeout waiting for Python chunk"
            } elseif ($result -ne $script:VS_OK) {
                throw "VS_WaitPy2PsChunk failed: $result"
            }
            
            # Read chunk from shared memory (ZERO-COPY READ!)
            $chunkPtr = [IntPtr]::Add($memBase, [int]$offset)
            $chunk = [byte[]]::new([int]$length)
            [System.Runtime.InteropServices.Marshal]::Copy($chunkPtr, $chunk, 0, [int]$length)
            
            $chunks.Add($chunk)
            $totalSize += $length
            
            # Acknowledge chunk
            $result = $native::VS_AckPy2PsChunk($handle)
            if ($result -ne $script:VS_OK) {
                throw "VS_AckPy2PsChunk failed: $result"
            }
            
            # Check if complete
            $isComplete = $native::VS_IsPy2PsComplete($handle)
            if ($isComplete -ne 1 -and $completionPollMs -gt 0) {
                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                while ($sw.ElapsedMilliseconds -lt $completionPollMs) {
                    if ($native::VS_IsPy2PsComplete($handle) -eq 1) {
                        $isComplete = 1
                        break
                    }
                    Start-Sleep -Milliseconds 1
                }
            }
            if ($isComplete -eq 1) {
                break
            }
        }
        
        # Combine chunks
        if ($chunks.Count -eq 1) {
            $data = $chunks[0]
        } else {
            $data = [byte[]]::new($totalSize)
            $pos = 0
            foreach ($chunk in $chunks) {
                [Array]::Copy($chunk, 0, $data, $pos, $chunk.Length)
                $pos += $chunk.Length
            }
        }
        
        # Store in variable (in caller's scope)
        Set-Variable -Name $VariableName -Value $data -Scope 1
    }
    finally {
        # PowerShell does NOT destroy channel - Python owns it
    }
}

# =============================================================================
# JOB-BASED ASYNC FUNCTIONS (for use with Shell)
# =============================================================================

function Start-VSBridgeJob {
    <#
    .SYNOPSIS
        Start PowerShell job that handles all async bridge operations
    
    .DESCRIPTION
        Creates a job that:
        - Loads the bridge module
        - Opens the channel
        - Waits for Python to send/receive
        - Handles all transfers asynchronously
        
        The job stays alive until stopped, handling multiple transfers.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ChannelName,
        
        [int]$FrameSizeMB = 64
    )
    
    # Get paths to pass to job
    $dllPath = $script:VSNativeDllPath
    $scriptPath = $PSCommandPath
    
    $jobScript = {
        param($DllPath, $ScriptPath, $ChannelName, $FrameSizeMB)
        
        # Load module in job context
        . $ScriptPath
        Initialize-VSNative -PreferredPath $DllPath
        
        # Open channel (Python creates it, we just open)
        $fullChannelName = "Local\$ChannelName"
        $channelHandle = [VS]::OpenChannel($fullChannelName)
        
        if ($channelHandle -eq [IntPtr]::Zero) {
            throw "Failed to open channel: $fullChannelName"
        }
        
        Write-Output "Job: Channel opened: $channelHandle"
        
        try {
            # Keep job alive, waiting for commands via job communication
            # For now, just keep it alive
            while ($true) {
                Start-Sleep -Milliseconds 100
                
                # Check if we should exit (parent will stop the job)
                if ($using:ShouldExit) {
                    break
                }
            }
        }
        finally {
            # Channel is owned by Python, don't close it
            Write-Output "Job: Exiting"
        }
    }
    
    $job = Start-Job -ScriptBlock $jobScript -ArgumentList $dllPath, $scriptPath, $ChannelName, $FrameSizeMB
    
    # Wait a moment for job to start
    Start-Sleep -Milliseconds 200
    
    return $job
}

function Stop-VSBridgeJob {
    <#
    .SYNOPSIS
        Stop the VS bridge job
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [int]$JobId
    )
    
    Stop-Job -Id $JobId -ErrorAction SilentlyContinue
    Remove-Job -Id $JobId -Force -ErrorAction SilentlyContinue
}

function Start-SendVariableToPythonJob {
    <#
    .SYNOPSIS
        Send variable to Python via ThreadJob (same process)
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ChannelName,
        
        [Parameter(Mandatory)]
        [object]$Variable,
        
    [int]$ChunkSizeMB = 4,
    [int]$TimeoutSeconds = 30,
    [ValidateSet('Local','Global')]
    [string]$Scope = 'Local',
        
        [string]$ScriptPath,
        [string]$DllPath
    )
    
    # Use explicit paths or fall back to current context
    if (-not $ScriptPath) {
        $ScriptPath = $PSCommandPath
        if (-not $ScriptPath) {
            $ScriptPath = $script:MyInvocation.MyCommand.Path
        }
    }
    
    if (-not $DllPath) {
        $DllPath = $script:VSNativeDllPath
    }
    
    $jobScript = {
        param($ScriptPath, $DllPath, $ChannelName, $Variable, $ChunkSizeMB, $TimeoutSeconds, $Scope)
        
        try {
            Write-Output "[ThreadJob] Starting..."
            Write-Output "[ThreadJob] ScriptPath: $ScriptPath"
            Write-Output "[ThreadJob] DllPath: $DllPath"
            Write-Output "[ThreadJob] ChannelName: $ChannelName"
            
            # Re-source the module in ThreadJob
            Write-Output "[ThreadJob] Sourcing module..."
            . $ScriptPath
            
            Write-Output "[ThreadJob] Initializing DLL..."
            Initialize-VSNative -PreferredPath $DllPath
            
            Write-Output "[ThreadJob] Calling Send-VariableToPython..."
            # Now we have access to functions and [VS] type
            Send-VariableToPython -ChannelName $ChannelName -Variable $Variable -ChunkSizeMB $ChunkSizeMB -TimeoutSeconds $TimeoutSeconds -Scope $Scope
            
            Write-Output "[ThreadJob] Send completed successfully"
        }
        catch {
            Write-Error "[ThreadJob] ERROR: $_"
            Write-Error "[ThreadJob] Stack: $($_.ScriptStackTrace)"
            throw
        }
    }
    
    # Start ThreadJob with parameters
    Start-ThreadJob -ScriptBlock $jobScript -ArgumentList $scriptPath, $dllPath, $ChannelName, $Variable, $ChunkSizeMB, $TimeoutSeconds, $Scope
}

function Start-ReceiveVariableFromPythonJob {
    <#
    .SYNOPSIS
        Receive variable from Python via ThreadJob (same process)
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ChannelName,
        
    [Parameter(Mandatory)]
    [string]$VariableName,
        
    [int]$TimeoutSeconds = 30,
    [ValidateSet('Local','Global')]
    [string]$Scope = 'Local',
        
        [string]$ScriptPath,
        [string]$DllPath
    )
    
    # Use explicit paths or fall back to current context
    if (-not $ScriptPath) {
        $ScriptPath = $PSCommandPath
        if (-not $ScriptPath) {
            $ScriptPath = $script:MyInvocation.MyCommand.Path
        }
    }
    
    if (-not $DllPath) {
        $DllPath = $script:VSNativeDllPath
    }
    
    $jobScript = {
        param($ScriptPath, $DllPath, $ChannelName, $VariableName, $TimeoutSeconds, $Scope)
        
        try {
            Write-Output "[ThreadJob-Receive] Starting..."
            
            # Re-source the module in ThreadJob
            Write-Output "[ThreadJob-Receive] Sourcing module..."
            . $ScriptPath
            
            Write-Output "[ThreadJob-Receive] Initializing DLL..."
            Initialize-VSNative -PreferredPath $DllPath
            
            Write-Output "[ThreadJob-Receive] Calling Receive-VariableFromPython..."
            # Receive data
            Receive-VariableFromPython -ChannelName $ChannelName -VariableName 'data' -TimeoutSeconds $TimeoutSeconds -Scope $Scope
            
            Write-Output "[ThreadJob-Receive] Received $($data.Length) bytes"
            # Return data from job
            return $data
        }
        catch {
            Write-Error "[ThreadJob-Receive] ERROR: $_"
            Write-Error "[ThreadJob-Receive] Stack: $($_.ScriptStackTrace)"
            throw
        }
    }
    
    # Start ThreadJob with parameters
    Start-ThreadJob -ScriptBlock $jobScript -ArgumentList $scriptPath, $dllPath, $ChannelName, $VariableName, $TimeoutSeconds, $Scope
}
