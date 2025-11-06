param(
    [Parameter(Mandatory = $true)] [string] $ChannelName,
    [Parameter(Mandatory = $true)] [int]    $FrameBytes,
    [Parameter(Mandatory = $false)] [string] $VariableName,
    [string] $Encoding = 'utf8',
    [ValidateSet('Export','Import')] [string] $Mode = 'Export',
    [UInt64] $PythonSequence = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$HeaderBytes = 128

if (-not ('VirtualShell.SharedMemoryCopyProfile' -as [type])) {
    Add-Type -Language CSharp -CompilerOptions "/unsafe" -ReferencedAssemblies "System.IO.MemoryMappedFiles" -PassThru -TypeDefinition @"
using System;
using System.IO.MemoryMappedFiles;

namespace VirtualShell {
    public static class SharedMemoryCopyProfile {
        public static unsafe void CopyTo(MemoryMappedViewAccessor accessor, long offset, byte[] source, int length) {
            if (length <= 0) return;
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (length > source.Length) throw new ArgumentOutOfRangeException(nameof(length));
            byte* basePtr = null;
            try {
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
                if (basePtr == null) throw new InvalidOperationException("Failed to acquire pointer to accessor.");
                byte* dstPtr = basePtr + accessor.PointerOffset + offset;
                fixed (byte* srcPtr = source) {
                    Buffer.MemoryCopy(srcPtr, dstPtr, length, length);
                }
            } finally {
                if (basePtr != null) accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        public static unsafe void CopyFrom(MemoryMappedViewAccessor accessor, long offset, byte[] destination, int length) {
            if (length <= 0) return;
            if (destination == null) throw new ArgumentNullException(nameof(destination));
            if (length > destination.Length) throw new ArgumentOutOfRangeException(nameof(length));
            byte* basePtr = null;
            try {
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
                if (basePtr == null) throw new InvalidOperationException("Failed to acquire pointer to accessor.");
                byte* srcPtr = basePtr + accessor.PointerOffset + offset;
                fixed (byte* dstPtr = destination) {
                    Buffer.MemoryCopy(srcPtr, dstPtr, length, length);
                }
            } finally {
                if (basePtr != null) accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
    }
}
"@
    | Out-Null
}

function Invoke-ProfileCopyToAccessor {
    param(
        [Parameter(Mandatory)] [System.IO.MemoryMappedFiles.MemoryMappedViewAccessor] $Accessor,
        [Parameter(Mandatory)] [long] $Offset,
        [Parameter(Mandatory)] [byte[]] $Source,
        [Parameter(Mandatory)] [int] $Length
    )
    if ($Length -le 0) { return }
    [VirtualShell.SharedMemoryCopyProfile]::CopyTo($Accessor, $Offset, $Source, $Length)
}

function Invoke-ProfileCopyFromAccessor {
    param(
        [Parameter(Mandatory)] [System.IO.MemoryMappedFiles.MemoryMappedViewAccessor] $Accessor,
        [Parameter(Mandatory)] [long] $Offset,
        [Parameter(Mandatory)] [byte[]] $Destination,
        [Parameter(Mandatory)] [int] $Length
    )
    if ($Length -le 0) { return }
    [VirtualShell.SharedMemoryCopyProfile]::CopyFrom($Accessor, $Offset, $Destination, $Length)
}

if ($Mode -eq 'Export' -and -not (Get-Command Resolve-VariableValue -ErrorAction SilentlyContinue)) {
    throw "Resolve-VariableValue is not loaded. Dot-source src/virtualshell/shared_memory_bridge.ps1 first."
}

switch ($Mode) {
    'Export' {
        if (-not $VariableName) {
            throw "Mode 'Export' requires -VariableName."
        }

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $val = Resolve-VariableValue -VariableName $VariableName
        $resolveMs = $sw.Elapsed.TotalMilliseconds

        $sw.Restart()
        $bytes = Get-StrictBytes -InputObject $val -Encoding $Encoding
        $strictMs = $sw.Elapsed.TotalMilliseconds

        $sw.Restart()
        $mmf = [System.IO.MemoryMappedFiles.MemoryMappedFile]::OpenExisting($ChannelName)
        $accessor = $mmf.CreateViewAccessor()
        try {
            $openMs = $sw.Elapsed.TotalMilliseconds

            $frameBytesHeader = $accessor.ReadUInt64(8)
            if ($frameBytesHeader -ne [uint64]$FrameBytes) {
                throw "Shared memory frame size mismatch. Header=$frameBytesHeader, expected=$FrameBytes"
            }

            $sw.Restart()
            $targetOffset = $HeaderBytes + $FrameBytes
            Invoke-ProfileCopyToAccessor -Accessor $accessor -Offset $targetOffset -Source $bytes -Length $bytes.Length
            $accessor.Write(40, [uint64]$bytes.Length) # powershell_length
            $seq = $accessor.ReadUInt64(24)
            $accessor.Write(24, [uint64]($seq + 1))      # powershell_seq
            $copyMs = $sw.Elapsed.TotalMilliseconds
        }
        finally {
            $accessor.Dispose()
            $mmf.Dispose()
        }

        [pscustomobject]@{
            Mode      = 'Export'
            ResolveMs = $resolveMs
            StrictMs  = $strictMs
            OpenMs    = $openMs
            CopyMs    = $copyMs
            TypeName  = $val.GetType().FullName
            BytesType = $bytes.GetType().FullName
            Length    = $bytes.Length
        }
        return
    }

    'Import' {
        $mmf = [System.IO.MemoryMappedFiles.MemoryMappedFile]::OpenExisting($ChannelName)
        $accessor = $mmf.CreateViewAccessor()
        try {
            $frameBytesHeader = $accessor.ReadUInt64(8)
            if ($frameBytesHeader -ne [uint64]$FrameBytes) {
                throw "Shared memory frame size mismatch. Header=$frameBytesHeader, expected=$FrameBytes"
            }

            if ($PythonSequence -le 0) {
                $PythonSequence = $accessor.ReadUInt64(16)
            }
            if ($PythonSequence -le 0) {
                throw "Channel '$ChannelName' has no Python sequence available to read."
            }

            $reported = $accessor.ReadUInt64(32)
            $length = [Math]::Min([uint64]$FrameBytes, $reported)

            if ($length -gt [uint64][int]::MaxValue) {
                throw "Length $length exceeds Int32::MaxValue, cannot allocate byte[]"
            }

            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            $buffer = [byte[]]::new([int]$length)
            $allocMs = $sw.Elapsed.TotalMilliseconds

            $sw.Restart()
            Invoke-ProfileCopyFromAccessor -Accessor $accessor -Offset $HeaderBytes -Destination $buffer -Length ([int]$length)
            $copyMs = $sw.Elapsed.TotalMilliseconds

            if ($VariableName) {
                $targetScope = 'Global'
                $targetName = $VariableName
                if ($VariableName -match '^(?<scope>global|script|local|private):(?<name>.+)$') {
                    $targetScope = $Matches.scope
                    $targetName = $Matches.name
                }
                Set-Variable -Name $targetName -Scope $targetScope -Value ([byte[]]$buffer)
            }

            [pscustomobject]@{
                Mode          = 'Import'
                PythonSeq     = $PythonSequence
                AllocMs       = $allocMs
                CopyMs        = $copyMs
                Length        = $length
                BytesType     = $buffer.GetType().FullName
                ReportedBytes = $reported
            }
        }
        finally {
            $accessor.Dispose()
            $mmf.Dispose()
        }
        return
    }
}

throw "Unsupported Mode '$Mode'."