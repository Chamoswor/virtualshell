# Ultra-fast zero-copy bridge - PowerShell side
# Supports ALL types with intelligent fallback:
# - byte[] → TRUE zero-copy (fastest)
# - Other types → Fast serialization then zero-copy transfer

class FastBridge {
    # Send ANY data type to Python (intelligent routing with auto-chunking)
    static [void] SendBytesNoWait([string]$ChannelName, [int]$FrameBytes, [object]$Data) {
        if ($null -eq $Data) {
            throw "Data cannot be null"
        }
        
        # Determine data type and convert to bytes if needed
        [byte[]]$bytes = $null
        
        if ($Data -is [byte[]]) {
            # Already bytes - zero-copy path
            $bytes = $Data
        }
        elseif ($Data -is [string]) {
            # String - encode to UTF-8
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($Data)
        }
        else {
            # Complex object - serialize with CliXml (fast, preserves type info)
            $xmlString = [System.Management.Automation.PSSerializer]::Serialize($Data, 2)
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($xmlString)
        }
        
        if ($bytes.Length -eq 0) {
            throw "Data serialized to zero bytes"
        }
        
        # Auto-chunk if data exceeds frame size
        if ($bytes.Length -gt $FrameBytes) {
            [FastBridge]::SendBytesChunkedNoWait($ChannelName, $FrameBytes, $bytes)
            return
        }
        
        # Use WriteArrayViaOffsetNoWait - fire and forget, Python manages lifecycle
        $channel = [SharedMemoryChannel]::new($ChannelName, $FrameBytes, 1)
        
        # Write data and signal Python - then EXIT without closing
        # Python owns the channel lifecycle and will close it when done
        $channel.WriteArrayViaOffsetNoWait($bytes)
        
        # That's it! Python will read the data and close the channel
        # We don't close here because that would destroy the event before Python sees it
    }
    
    # Send large data in chunks (PowerShell → Python)
    static hidden [void] SendBytesChunked([string]$ChannelName, [int]$FrameBytes, [byte[]]$Data) {
        $totalSize = $Data.Length
        $chunkSize = [int]([Math]::Min($FrameBytes, 128MB))  # Use frame size or 128 MB
        $numChunks = [int]([Math]::Ceiling($totalSize / $chunkSize))
        
        # Send metadata: total size for Python to pre-allocate
        $metaChannel = [SharedMemoryChannel]::new("${ChannelName}_meta", 1024, 1)
        try {
            $metaStr = "CHUNKED:" + $totalSize + ":" + $chunkSize + ":" + $numChunks
            $metaBytes = [System.Text.Encoding]::UTF8.GetBytes($metaStr)
            $metaChannel.WriteArrayViaOffset($metaBytes, 5000)
        } finally {
            $metaChannel.Close()
        }
        
        # Send chunks using same channel (re-use for efficiency)
        $channel = [SharedMemoryChannel]::new($ChannelName, $chunkSize, 1)
        try {
            $native = $channel.GetNativeInteropType()
            $shmBase = $native::VS_GetSharedMemoryBase($channel.Handle)
            if ($shmBase -eq [IntPtr]::Zero) {
                throw "Failed to get shared memory base"
            }
            
            $headerSize = 128
            $ps2pyOffset = $headerSize + $chunkSize
            
            for ($i = 0; $i -lt $numChunks; $i++) {
                $start = $i * $chunkSize
                $end = [Math]::Min($start + $chunkSize, $totalSize)
                $currentChunkSize = [int]($end - $start)
                
                # Get pointer to PS→Python buffer
                $ps2pyPtr = [IntPtr]::Add($shmBase, [int]$ps2pyOffset)
                
                # Copy chunk to shared memory
                [System.Runtime.InteropServices.Marshal]::Copy($Data, $start, $ps2pyPtr, $currentChunkSize)
                
                # Set metadata to signal chunk is ready
                $status = $native::VS_SetDataOffset($channel.Handle, [UInt64]$ps2pyOffset, [UInt64]$currentChunkSize)
                if ($status -ne 0) {
                    $chunkNum = $i + 1
                    throw "Chunk ${chunkNum}/${numChunks} - VS_SetDataOffset failed with status $status"
                }
                
                # Wait for Python to acknowledge before next chunk
                $deadline = (Get-Date).AddSeconds(30)
                $acked = $false
                
                while ((Get-Date) -lt $deadline) {
                    $ackStatus = $native::VS_GetDataOffset($channel.Handle, [ref][UInt64]0, [ref][UInt64]0, [ref][UInt64]0)
                    if ($ackStatus -eq 1) {  # VS_CONSUMED
                        $acked = $true
                        break
                    }
                    Start-Sleep -Milliseconds 5
                }
                
                if (-not $acked) {
                    $chunkNum = $i + 1
                    throw "Chunk ${chunkNum}/${numChunks} - Python did not acknowledge"
                }
            }
        } finally {
            $channel.Close()
        }
    }
    
    # Send large data in chunks - NoWait version (Python manages lifecycle)
    # Uses SAME channel for all chunks to avoid RAM issues
    # REDESIGNED: Uses new C++ chunked transfer API (VS_BeginChunkedTransfer + VS_SendChunk)
    static hidden [void] SendBytesChunkedNoWait([string]$ChannelName, [int]$FrameBytes, [byte[]]$Data) {
        $totalSize = $Data.Length
        $chunkSize = [int]([Math]::Min($FrameBytes, 4MB))  # 4 MB chunks
        $numChunks = [int]([Math]::Ceiling($totalSize / $chunkSize))
        
        # Open ONE channel for entire transfer
        $channel = [SharedMemoryChannel]::new($ChannelName, $chunkSize, 1)
        $native = $channel.GetNativeInteropType()
        
        try {
            # Begin chunked transfer (sets metadata in header)
            $status = $native::VS_BeginChunkedTransfer($channel.Handle, [UInt64]$totalSize, [UInt64]$chunkSize)
            if ($status -ne 0) {
                throw "VS_BeginChunkedTransfer failed with status $status"
            }
            
            # First chunk contains metadata header for compatibility
            $metaHeader = "CHUNKED|$totalSize|$chunkSize|$numChunks|"
            $metaBytes = [System.Text.Encoding]::UTF8.GetBytes($metaHeader)
            $headerLen = $metaBytes.Length
            
            # Create first chunk with header + first data
            $firstChunkDataSize = [Math]::Min($chunkSize - $headerLen, $totalSize)
            $firstChunk = New-Object byte[] ($headerLen + $firstChunkDataSize)
            
            # Copy header
            [System.Array]::Copy($metaBytes, 0, $firstChunk, 0, $headerLen)
            
            # Copy first chunk of data
            if ($firstChunkDataSize -gt 0) {
                [System.Array]::Copy($Data, 0, $firstChunk, $headerLen, $firstChunkDataSize)
            }
            
            # Send first chunk using new API (chunk_index=0, timeout=5000ms)
            $status = $native::VS_SendChunk($channel.Handle, [UInt32]0, $firstChunk, [UInt64]$firstChunk.Length, [UInt32]5000)
            if ($status -ne 0) {
                throw "VS_SendChunk (chunk 0) failed with status $status"
            }
            
            # Send remaining chunks
            $dataSent = $firstChunkDataSize
            
            for ($i = 1; $i -lt $numChunks; $i++) {
                $start = $dataSent
                $end = [Math]::Min($start + $chunkSize, $totalSize)
                $currentChunkSize = [int]($end - $start)
                
                # Extract chunk
                $chunk = New-Object byte[] $currentChunkSize
                [System.Array]::Copy($Data, $start, $chunk, 0, $currentChunkSize)
                
                # Send chunk with ACK wait (VS_SendChunk waits internally)
                $status = $native::VS_SendChunk($channel.Handle, [UInt32]$i, $chunk, [UInt64]$currentChunkSize, [UInt32]5000)
                if ($status -eq 1) {
                    throw "VS_SendChunk (chunk $i) timed out"
                }
                elseif ($status -ne 0) {
                    throw "VS_SendChunk (chunk $i) failed with status $status"
                }
                
                $dataSent = $end
            }
        }
        finally {
            # Don't close channel - Python will close it when done reading all chunks
            # This is critical: channel.Close() destroys events before Python finishes
        }
    }
    
    # Original blocking version (for standalone use)
    static [void] SendBytes([string]$ChannelName, [int]$FrameBytes, [byte[]]$Data) {
        if ($null -eq $Data -or $Data.Length -eq 0) {
            throw "Data cannot be null or empty"
        }
        
        $channel = [SharedMemoryChannel]::new($ChannelName, $FrameBytes, 1)
        
        try {
            # Use fast offset-based zero-copy transfer
            $channel.WriteArrayViaOffset($Data, 30000)
        }
        finally {
            $channel.Close()
        }
    }
    
    # Receive bytes from Python
    static [byte[]] ReceiveBytes([string]$ChannelName, [int]$FrameBytes) {
        $channel = [SharedMemoryChannel]::new($ChannelName, $FrameBytes, 1)
        
        try {
            # Read from Python→PowerShell buffer
            return $channel.ReadFromPython(30000)
        }
        finally {
            $channel.Close()
        }
    }
}
