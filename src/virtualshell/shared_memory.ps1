# src/virtualshell/shared_memory.ps1

class SharedMemoryChannel {
    [System.IO.MemoryMappedFiles.MemoryMappedFile]$mmf
    [System.IO.MemoryMappedFiles.MemoryMappedViewAccessor]$accessor
    [int]$n_slots
    [int]$frame_bytes
    
    # Størrelsen på headeren (to uint64-verdier)
    [int]$header_size = 16 
    
    # Start-offset for hver buffer
    [long]$p2p_buffer_offset # Python -> PS
    [long]$ps2p_buffer_offset # PS -> Python

    SharedMemoryChannel([string]$Name, [int]$NumSlots, [int]$FrameBytes) {
        $this.n_slots = $NumSlots
        $this.frame_bytes = $FrameBytes
        
        $single_buffer_size = $NumSlots * $FrameBytes
        $this.p2p_buffer_offset = $this.header_size
        $this.ps2p_buffer_offset = $this.header_size + $single_buffer_size

        # Retry-løkke for å håndtere race conditions ved oppstart
        $max_retries = 5
        $retry_delay_ms = 20
        for ($i = 0; $i -lt $max_retries; $i++) {
            try {
                $this.mmf = [System.IO.MemoryMappedFiles.MemoryMappedFile]::OpenExisting($Name)
                $this.accessor = $this.mmf.CreateViewAccessor()
                # Suksess, gå ut av løkken
                return
            } catch {
                if ($i -eq ($max_retries - 1)) {
                    # Siste forsøk feilet, kast exception videre
                    throw "Failed to open shared memory '$Name' after $max_retries retries. Last error: $_"
                }
                Start-Sleep -Milliseconds $retry_delay_ms
            }
        }
    }

    # Hent siste sekvensnummer FRA Python
    [uint64] GetPythonSeq() {
        return $this.accessor.ReadUInt64(0) # p2p_seq er først i headeren
    }

    # Hent siste sekvensnummer FRA PowerShell (dvs. det vi har skrevet)
    [uint64] GetPowerShellSeq() {
        return $this.accessor.ReadUInt64(8) # ps2p_seq er på offset 8
    }

    # Les en spesifikk frame FRA Python
    [byte[]] ReadFromPython([uint64]$SequenceNumber) {
        $currentSeq = $this.GetPythonSeq()
        if ($SequenceNumber -ge $currentSeq) {
            throw "Sequence number $SequenceNumber is not yet available from Python (current is $currentSeq)."
        }

        $slot = $SequenceNumber % $this.n_slots
        $offset = $this.p2p_buffer_offset + ($slot * $this.frame_bytes)

        $frame = New-Object byte[]($this.frame_bytes)
        $this.accessor.ReadArray($offset, $frame, 0, $this.frame_bytes)
        
        return $frame
    }

    # Skriv data TIL Python
    [void] WriteToPython([byte[]]$Data) {
        if ($Data.Length -ne $this.frame_bytes) {
            throw "Data length must be exactly $($this.frame_bytes) bytes."
        }

        # Lås for å sikre atomisk oppdatering av sekvenstelleren
        $psSeq = $this.GetPowerShellSeq()
        $slot = $psSeq % $this.n_slots
        $offset = $this.ps2p_buffer_offset + ($slot * $this.frame_bytes)

        # Skriv data
        $this.accessor.WriteArray($offset, $Data, 0, $Data.Length)

        # Publiser ved å øke sekvenstelleren
        $this.accessor.Write(8, [uint64]($psSeq + 1))
    }

    Dispose() {
        if ($this.accessor) { $this.accessor.Dispose() }
        if ($this.mmf) { $this.mmf.Dispose() }
    }
}