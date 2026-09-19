# Zero-Copy Bridge v3 - PowerShell side (cross-platform)
#
# Transport: a file-backed memory-mapped file shared with Python.
# Python owns the file lifecycle; this side opens the same file via
# System.IO.MemoryMappedFiles, which works on Windows, Linux and macOS
# under PowerShell 7. No native DLL is required.
#
# Layout (little-endian; must match _MmapChannel in zero_copy_bridge_shell.py):
#   0    4   magic 'VSZB'
#   4    4   version (3)
#   8    8   frame_bytes (data region size per direction)
#   16   64  py2ps direction block (Python writes, PowerShell reads)
#   80   64  ps2py direction block (PowerShell writes, Python reads)
#   256  frame_bytes  py2ps data region
#   256+frame_bytes  frame_bytes  ps2py data region
#
# Direction block fields (relative offsets):
#   0  int64 total_size    8  int64 chunk_size   16 int32 num_chunks
#   20 int32 chunk_index   24 int64 chunk_length
#   32 int32 chunk_ready   36 int32 transfer_done  40 int32 error
#
# Protocol per direction (single writer, single reader, one chunk in flight):
#   writer: reset block; per chunk: copy payload, set index/length, ready=1;
#           wait ready==0 (ack); after last ack set transfer_done=1.
#   reader: wait ready==1 -> copy payload -> ready=0; done when all chunks read
#           (or transfer_done==1 with num_chunks==0 for empty payloads).
#
# NOTE: this script is dot-sourced into the user's session, so it must not
# mutate session-wide state such as $ErrorActionPreference.

function Initialize-VsZcb {
    <#
    .SYNOPSIS
        Open the shared memory-mapped file created by Python.
    .PARAMETER Path
        Path to the channel file (created and owned by the Python side).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if (-not ('VirtualShell.ZcbChannel' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace VirtualShell {
    public sealed class ZcbChannel : IDisposable {
        const long HEADER_SIZE = 256;
        const uint MAGIC = 0x425A5356;   // 'VSZB' read as little-endian uint32
        const int VERSION = 3;

        const long BLK_PY2PS = 16;       // Python -> PowerShell (we read)
        const long BLK_PS2PY = 80;       // PowerShell -> Python (we write)

        // Field offsets within a direction block.
        const long F_TOTAL = 0, F_CHUNK = 8, F_NUM = 16, F_IDX = 20,
                   F_LEN = 24, F_READY = 32, F_DONE = 36, F_ERR = 40;

        const int ERR_PS_ABORT = 2;

        readonly MemoryMappedFile _mmf;
        readonly MemoryMappedViewAccessor _acc;
        readonly long _frame;

        public ZcbChannel(string path) {
            var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            _mmf = MemoryMappedFile.CreateFromFile(
                fs, null, 0, MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None, false);
            _acc = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);

            if (_acc.ReadUInt32(0) != MAGIC)
                throw new InvalidDataException("Not a virtualshell zero-copy channel file");
            if (_acc.ReadInt32(4) != VERSION)
                throw new InvalidDataException("Channel protocol version mismatch");
            _frame = _acc.ReadInt64(8);
        }

        // ------------- Python -> PowerShell (reader role) -------------
        public byte[] ReceiveFromPython(int timeoutMs) {
            long blk = BLK_PY2PS, dataOff = HEADER_SIZE;
            var sw = Stopwatch.StartNew();
            byte[] result = null;
            long written = 0;
            int received = 0, num = -1;

            while (true) {
                Thread.MemoryBarrier();
                int err = _acc.ReadInt32(blk + F_ERR);
                if (err != 0)
                    throw new IOException("Python side aborted the transfer (error=" + err + ")");

                if (_acc.ReadInt32(blk + F_READY) == 1) {
                    if (result == null) {
                        num = _acc.ReadInt32(blk + F_NUM);
                        result = new byte[_acc.ReadInt64(blk + F_TOTAL)];
                    }
                    int len = (int)_acc.ReadInt64(blk + F_LEN);
                    _acc.ReadArray(dataOff, result, (int)written, len);
                    written += len;
                    received++;
                    Thread.MemoryBarrier();
                    _acc.Write(blk + F_READY, (int)0);   // ack
                    if (received == num) break;
                    continue;                             // next chunk, no sleep
                }

                if (_acc.ReadInt32(blk + F_DONE) == 1) {
                    // Covers the empty-payload case (num_chunks == 0).
                    if (result == null)
                        result = new byte[_acc.ReadInt64(blk + F_TOTAL)];
                    if (received >= _acc.ReadInt32(blk + F_NUM)) break;
                }

                if (sw.ElapsedMilliseconds > timeoutMs) {
                    _acc.Write(blk + F_ERR, ERR_PS_ABORT);
                    throw new TimeoutException("Timed out waiting for chunk from Python");
                }
                Thread.Sleep(1);
            }
            return result;
        }

        // ------------- PowerShell -> Python (writer role) -------------
        public void SendToPython(byte[] data, long chunkSize, int timeoutMs) {
            long blk = BLK_PS2PY, dataOff = HEADER_SIZE + _frame;
            if (chunkSize <= 0 || chunkSize > _frame) chunkSize = _frame;

            int num = data.Length == 0 ? 0 : (int)(((long)data.Length + chunkSize - 1) / chunkSize);

            _acc.Write(blk + F_TOTAL, (long)data.Length);
            _acc.Write(blk + F_CHUNK, chunkSize);
            _acc.Write(blk + F_NUM, num);
            _acc.Write(blk + F_IDX, (int)0);
            _acc.Write(blk + F_LEN, (long)0);
            _acc.Write(blk + F_READY, (int)0);
            _acc.Write(blk + F_DONE, (int)0);
            _acc.Write(blk + F_ERR, (int)0);
            Thread.MemoryBarrier();

            var sw = Stopwatch.StartNew();
            long pos = 0;
            for (int i = 0; i < num; i++) {
                int len = (int)Math.Min(chunkSize, data.Length - pos);
                _acc.WriteArray(dataOff, data, (int)pos, len);
                _acc.Write(blk + F_IDX, i);
                _acc.Write(blk + F_LEN, (long)len);
                Thread.MemoryBarrier();
                _acc.Write(blk + F_READY, (int)1);

                while (true) {
                    Thread.MemoryBarrier();
                    int err = _acc.ReadInt32(blk + F_ERR);
                    if (err != 0)
                        throw new IOException("Python side aborted the transfer (error=" + err + ")");
                    if (_acc.ReadInt32(blk + F_READY) == 0) break;   // acked
                    if (sw.ElapsedMilliseconds > timeoutMs) {
                        _acc.Write(blk + F_ERR, ERR_PS_ABORT);
                        throw new TimeoutException("Timed out waiting for ack from Python");
                    }
                    Thread.Sleep(0);
                }
                pos += len;
            }
            Thread.MemoryBarrier();
            _acc.Write(blk + F_DONE, (int)1);
        }

        public void Dispose() {
            _acc.Dispose();
            _mmf.Dispose();
        }
    }
}
'@
    }

    if ($global:__VsZcbChannel) {
        try { $global:__VsZcbChannel.Dispose() } catch {}
    }
    $global:__VsZcbChannel = [VirtualShell.ZcbChannel]::new($Path)
}

function Close-VsZcb {
    <#
    .SYNOPSIS
        Release the memory-mapped file so Python can delete it.
    #>
    [CmdletBinding()]
    param()

    if ($global:__VsZcbChannel) {
        try { $global:__VsZcbChannel.Dispose() } catch {}
        $global:__VsZcbChannel = $null
    }
}

function Send-VariableToPython {
    <#
    .SYNOPSIS
        Send a PowerShell value to Python over the shared channel.
    .DESCRIPTION
        byte[] values are sent as-is. Anything else is serialized to CliXml
        via PSSerializer first, so Python can parse it with PSObject.from_bytes.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [AllowNull()]
        [object]$Variable,

        [int]$ChunkSizeMB = 4,
        [int]$TimeoutSeconds = 30,
        [int]$Depth = 1
    )

    if (-not $global:__VsZcbChannel) {
        throw "Zero-copy channel is not initialized. Call Initialize-VsZcb first."
    }

    if ($Variable -is [byte[]]) {
        $bytes = [byte[]]$Variable
    } else {
        $xml = [System.Management.Automation.PSSerializer]::Serialize($Variable, $Depth)
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($xml)
    }

    $chunkBytes = [long]$ChunkSizeMB * 1024 * 1024
    $global:__VsZcbChannel.SendToPython($bytes, $chunkBytes, $TimeoutSeconds * 1000)
}

function Receive-VariableFromPython {
    <#
    .SYNOPSIS
        Receive bytes from Python into a global PowerShell variable.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$VariableName,

        [int]$TimeoutSeconds = 30
    )

    if (-not $global:__VsZcbChannel) {
        throw "Zero-copy channel is not initialized. Call Initialize-VsZcb first."
    }

    $bytes = $global:__VsZcbChannel.ReceiveFromPython($TimeoutSeconds * 1000)
    Set-Variable -Name $VariableName -Value $bytes -Scope Global
}
