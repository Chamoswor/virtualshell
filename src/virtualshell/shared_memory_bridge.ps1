# src/virtualshell/shared_memory_bridge.ps1

param(
    [string]$dll_path = ''
)

if (-not (Get-Command -Name Initialize-VSSharedMemoryNative -ErrorAction SilentlyContinue)) {
    . (Join-Path $PSScriptRoot 'shared_memory.ps1')
}

if ($dll_path) {
    Initialize-VSSharedMemoryNative -PreferredPath $dll_path
} else {
    Initialize-VSSharedMemoryNative
}

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Zero-copy wrapper that keeps data in shared memory
class SharedMemoryBuffer {
    [SharedMemoryChannel]$Channel
    [string]$ChannelName
    [int]$FrameBytes
    [long]$Length
    hidden [byte[]]$_cachedBytes = $null

    SharedMemoryBuffer([string]$channelName, [int]$frameBytes, [long]$length, [SharedMemoryChannel]$channel) {
        $this.ChannelName = $channelName
        $this.FrameBytes = $frameBytes
        $this.Length = $length
        $this.Channel = $channel
    }

    # Lazy read - only copy when actually accessed
    [byte[]] GetBytes() {
        if ($null -eq $this._cachedBytes) {
            Write-Host "[SharedMemoryBuffer] Lazy-loading bytes from shared memory..."
            $this._cachedBytes = $this.Channel.ReadFromPython(5000)
        }
        return $this._cachedBytes
    }

    # For direct export without copying
    [void] ExportToChannel([string]$targetChannelName, [int]$targetFrameBytes) {
        # If exporting to same channel, just ensure PowerShell sequence is updated
        if ($targetChannelName -eq $this.ChannelName -and $targetFrameBytes -eq $this.FrameBytes) {
            Write-Host "[SharedMemoryBuffer] Same-channel zero-copy - no data movement needed"
            # Data is already in shared memory from import, just verify sequence
            $psSeq = $this.Channel.GetPowerShellSeq()
            Write-Host "[SharedMemoryBuffer] Current PS sequence: $psSeq"
            return
        }
        
        Write-Host "[SharedMemoryBuffer] Cross-channel export - must copy"
        $bytes = $this.GetBytes()
        $targetChannel = Get-OrCreateSharedMemoryChannel -ChannelName $targetChannelName -FrameBytes $targetFrameBytes
        $targetChannel.WriteToPython($bytes, 5000)
    }
}

function Get-SharedMemoryEncoding {
    param(
        [Parameter(Mandatory = $false)]
        [string]$Name = 'utf8'
    )

    try {
        return [System.Text.Encoding]::GetEncoding($Name)
    } catch {
        throw "SharedMemoryBridge: Unknown encoding '$Name'."
    }
}

function Import-SharedMemoryData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)] [string]$ChannelName,
        [Parameter(Mandatory = $true)] [int]$FrameBytes,
        [ValidateSet('Bytes','String','Json','Binary','ZeroCopy')] [string]$Format = 'Bytes',
        [string]$Encoding = 'utf8',
        [string]$VariableName
    )

    Write-Host "[Import-SharedMemoryData] Creating channel: $ChannelName, FrameBytes: $FrameBytes, Format: $Format"
    $channel = [SharedMemoryChannel]::new($ChannelName, $FrameBytes, 1)
    
    # For ZeroCopy mode, don't read yet - just create the wrapper
    if ($Format -eq 'ZeroCopy') {
        Write-Host "[Import-SharedMemoryData] Zero-copy mode - creating buffer wrapper"
        # Store in cache to prevent disposal
        if (-not (Test-Path variable:global:__vshell_channel_cache)) {
            $global:__vshell_channel_cache = [System.Collections.Hashtable]::new()
        }
        $cacheKey = "${ChannelName}:${FrameBytes}"
        $global:__vshell_channel_cache[$cacheKey] = $channel
        
        $buffer = [SharedMemoryBuffer]::new($ChannelName, $FrameBytes, $FrameBytes, $channel)
        
        if ($PSBoundParameters.ContainsKey('VariableName')) {
            Write-Host "[Import-SharedMemoryData] Setting zero-copy variable: $VariableName"
            $targetScope = 'Global'
            $targetName = $VariableName
            if ($VariableName -match '^(?<scope>global|script|private|local):(?<name>.+)$') {
                $targetScope = $Matches.scope
                $targetName = $Matches.name
            }
            Set-Variable -Name $targetName -Value $buffer -Scope $targetScope
        }
        return
    }
    
    # Original copy-based approach
    try {
        Write-Host "[Import-SharedMemoryData] Calling ReadFromPython..."
        [byte[]]$raw = $channel.ReadFromPython(5000)
        Write-Host "[Import-SharedMemoryData] Read $($raw.Length) bytes"
        Write-Host "[Import-SharedMemoryData] Read $($raw.Length) bytes"
        if ($raw.Length -eq 0) { $raw = [byte[]]::new(0) }
        if ($raw.Length -gt $FrameBytes) {
            throw "Import-SharedMemoryData: payload $($raw.Length) exceeds frame bytes $FrameBytes"
        }

        switch ($Format) {
            'Bytes' { $value = $raw }
            'String' {
                $enc = Get-SharedMemoryEncoding -Name $Encoding
                $value = $enc.GetString($raw)
            }
            'Json' {
                if ($raw.Length -eq 0) {
                    $value = $null
                    break
                }
                $enc = Get-SharedMemoryEncoding -Name $Encoding
                $json = $enc.GetString($raw)
                if ([string]::IsNullOrWhiteSpace($json)) {
                    $value = $null
                } else {
                    $value = $json | ConvertFrom-Json -Depth 64
                }
            }
            'Binary' { $value = $raw }
        }

        if ($PSBoundParameters.ContainsKey('VariableName')) {
            Write-Host "[Import-SharedMemoryData] Setting variable: $VariableName"
            $targetScope = 'Global'
            $targetName = $VariableName
            if ($VariableName -match '^(?<scope>global|script|private|local):(?<name>.+)$') {
                $targetScope = $Matches.scope
                $targetName = $Matches.name
            }
            Write-Host "[Import-SharedMemoryData] Target scope: $targetScope, name: $targetName"
            Set-Variable -Name $targetName -Value $value -Scope $targetScope
            Write-Host "[Import-SharedMemoryData] Variable set successfully"
        }
    }
    catch {
        Write-Host "[Import-SharedMemoryData] ERROR: $_"
        Write-Host "[Import-SharedMemoryData] Stack trace: $($_.ScriptStackTrace)"
        throw
    }
    finally {
        $channel.Dispose()
    }
}

function Get-OrCreateSharedMemoryChannel {
    param(
        [Parameter(Mandatory)][string]$ChannelName,
        [Parameter(Mandatory)][int]$FrameBytes
    )

    Write-Host "[Get-OrCreateSharedMemoryChannel] ChannelName: $ChannelName, FrameBytes: $FrameBytes"

    if (-not (Test-Path variable:global:__vshell_channel_cache)) {
        $global:__vshell_channel_cache = [System.Collections.Hashtable]::new()
    }

    $cacheKey = "${ChannelName}:${FrameBytes}"
    $channel = $global:__vshell_channel_cache[$cacheKey]

    if ($channel) {
        Write-Host "[Get-OrCreateSharedMemoryChannel] Found cached channel"
        try {
            $null = $channel.GetPowerShellSeq()
            return $channel
        } catch {
            Write-Host "[Get-OrCreateSharedMemoryChannel] Cached channel invalid, recreating: $_"
            try { $channel.Dispose() } catch { }
            $global:__vshell_channel_cache.Remove($cacheKey)
        }
    }

    Write-Host "[Get-OrCreateSharedMemoryChannel] Creating new channel"
    try {
        $channel = [SharedMemoryChannel]::new($ChannelName, $FrameBytes, 1)
        $global:__vshell_channel_cache[$cacheKey] = $channel
        Write-Host "[Get-OrCreateSharedMemoryChannel] Channel created successfully"
        return $channel
    } catch {
        Write-Host "[Get-OrCreateSharedMemoryChannel] ERROR creating channel: $_"
        Write-Host "[Get-OrCreateSharedMemoryChannel] Stack: $($_.ScriptStackTrace)"
        throw
    }
}

function Export-SharedMemoryData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)] [string]$ChannelName,
        [Parameter(Mandatory = $true)] [int]$FrameBytes,
        [Parameter(Mandatory = $true)] [string]$Command,
    [ValidateSet('Bytes','String','Json','Binary')] [string]$Format = 'Json',
        [string]$Encoding = 'utf8'
    )

    $scriptBlock = [ScriptBlock]::Create($Command)
    $result = $scriptBlock.InvokeReturnAsIs()

    switch ($Format) {
        'Bytes' {
            if ($null -eq $result) {
                $bytes = [byte[]]::new(0)
            } elseif ($result -is [byte[]]) {
                $bytes = $result
            } else {
                throw "Command must return byte[] when Format=Bytes."
            }
        }
        'String' {
            $enc = Get-SharedMemoryEncoding -Name $Encoding
            $text = [string]$result
            $bytes = $enc.GetBytes($text)
        }
        'Json' {
            $enc = Get-SharedMemoryEncoding -Name $Encoding
            $json = ConvertTo-Json -InputObject $result -Depth 64 -Compress
            $bytes = $enc.GetBytes($json)
        }
        'Binary' {
            if ($null -eq $result) {
                $bytes = [byte[]]::new(0)
            } elseif ($result -is [byte[]]) {
                $bytes = $result
            } elseif ($result -is [System.IO.Stream]) {
                $memory = [System.IO.MemoryStream]::new()
                try {
                    $result.CopyTo($memory)
                    $bytes = $memory.ToArray()
                }
                finally {
                    $memory.Dispose()
                }
            } elseif ($result -is [System.Array]) {
                try {
                    $bytes = [byte[]]$result
                } catch {
                    $bytes = $null
                }
                if ($null -eq $bytes) {
                    $list = [System.Collections.Generic.List[byte]]::new()
                    foreach ($item in $result) {
                        $list.Add([byte]$item)
                    }
                    $bytes = $list.ToArray()
                }
            } elseif ($result -is [System.Collections.Generic.IEnumerable[byte]]) {
                $list = [System.Collections.Generic.List[byte]]::new()
                foreach ($item in $result) {
                    $list.Add($item)
                }
                $bytes = $list.ToArray()
            } else {
                $enc = Get-SharedMemoryEncoding -Name $Encoding
                $json = ConvertTo-Json -InputObject $result -Depth 64 -Compress
                $bytes = $enc.GetBytes($json)
            }
        }
    }

    if ($bytes.Length -gt $FrameBytes) {
        throw "Payload length $($bytes.Length) exceeds frame capacity $FrameBytes."
    }

    $channel = Get-OrCreateSharedMemoryChannel -ChannelName $ChannelName -FrameBytes $FrameBytes

    try {
        $channel.WriteToPython($bytes)
    } catch {
        throw "Export-SharedMemoryData: Failed to write to shared memory channel: $_"
    }

    Write-Output $bytes.Length
}

function Convert-FromBinaryData {
    param (
        [Parameter(Mandatory = $true)] $Bytes,
        [ValidateSet('Bytes','String','Json','Binary')] [string]$Format = 'Json',
        [string]$Encoding = 'utf8'
    )

    $enc = Get-SharedMemoryEncoding -Name $Encoding
    switch ($Format) {
        'Bytes' { return $Bytes }
        'String' { return $enc.GetString($Bytes) }
        'Json' {
            $json = $enc.GetString($Bytes)
            return $json | ConvertFrom-Json -Depth 64
        }
        'Binary' { return $Bytes }
    }
}

function Get-StrictBytes {
    param([Parameter(Mandatory)] $InputObject,
          [string] $Encoding = 'utf8')
    if ($null -eq $InputObject) { return [byte[]]::new(0) }

    if ($InputObject -is [byte[]]) { return $InputObject }

    if ($InputObject -is [System.IO.Stream]) {
        $ms = [System.IO.MemoryStream]::new()
        try { $InputObject.CopyTo($ms); return $ms.ToArray() } finally { $ms.Dispose() }
    }

    if ($InputObject -is [System.Collections.Generic.IEnumerable[byte]]) {
        $list = [System.Collections.Generic.List[byte]]::new()
        foreach ($b in $InputObject) { $list.Add([byte]$b) }
        return $list.ToArray()
    }

    if ($InputObject -is [System.Array]) {
        try { return [byte[]]$InputObject } catch {
            $list = [System.Collections.Generic.List[byte]]::new()
            foreach ($b in $InputObject) { $list.Add([byte]$b) }
            return $list.ToArray()
        }
    }

    if ($InputObject -is [string]) {
        $enc = [System.Text.Encoding]::GetEncoding($Encoding)
        return $enc.GetBytes([string]$InputObject)
    }

    throw "Get-StrictBytes: Unsupported type '$($InputObject.GetType().FullName)'. This path forbids JSON."
}

function Export-SharedMemoryBytes {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $ChannelName,
        [Parameter(Mandatory)] [int]    $FrameBytes,
        [Parameter(Mandatory)] [string] $Command,
        [string] $Encoding = 'utf8'
    )

    $sb = [ScriptBlock]::Create($Command)
    $result = $sb.InvokeReturnAsIs()

    $bytes = Get-StrictBytes -InputObject $result -Encoding $Encoding
    $len = $bytes.Length
    if ($len -gt $FrameBytes) { throw "Export-SharedMemoryBytes: payload $len > frame $FrameBytes" }

    $channel = Get-OrCreateSharedMemoryChannel -ChannelName $ChannelName -FrameBytes $FrameBytes

    try { $channel.WriteToPython($bytes) } catch {
        throw "Export-SharedMemoryBytes: Failed to write to shared memory channel: $_"
    }

    Write-Output $len
}

function Resolve-VariableValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $VariableName
    )

    $scopesToCheck = @('Global','Script','Local')
    $name = $VariableName

    if ($VariableName -match '^(?<scope>global|script|local|private):(?<name>.+)$') {
        $scopesToCheck = @($Matches.scope)
        $name = $Matches.name
    }

    foreach ($scope in $scopesToCheck) {
        try {
            $var = Get-Variable -Name $name -Scope $scope -ErrorAction Stop
            if ($null -ne $var) {
                return $var.Value
            }
        } catch {
            continue
        }
    }

    throw "Resolve-VariableValue: Variable '$VariableName' not found in $($scopesToCheck -join '/')."
}

function Export-SharedMemoryVarBytes {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $ChannelName,
        [Parameter(Mandatory)] [int]    $FrameBytes,
        [Parameter(Mandatory)] [string] $VariableName,
        [string] $Encoding = 'utf8'
    )

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $val   = Resolve-VariableValue -VariableName $VariableName
    $resolveMs = $sw.Elapsed.TotalMilliseconds
    Write-Host "[Export] Resolve variable: $resolveMs ms"
    
    # Fast path for zero-copy buffers
    if ($val -is [SharedMemoryBuffer]) {
        Write-Host "[Export] Detected SharedMemoryBuffer - using zero-copy path"
        $sw.Restart()
        $val.ExportToChannel($ChannelName, $FrameBytes)
        $writeMs = $sw.Elapsed.TotalMilliseconds
        Write-Host "[Export] Zero-copy export: $writeMs ms"
        Write-Output $val.Length
        return
    }
    
    $sw.Restart()
    $bytes = Get-StrictBytes -InputObject $val -Encoding $Encoding
    $bytesMs = $sw.Elapsed.TotalMilliseconds
    Write-Host "[Export] Get bytes (len=$($bytes.Length)): $bytesMs ms"

    $len = $bytes.Length
    if ($len -gt $FrameBytes) { throw "Export-SharedMemoryVarBytes: payload $len > frame $FrameBytes" }

    $sw.Restart()
    $channel = Get-OrCreateSharedMemoryChannel -ChannelName $ChannelName -FrameBytes $FrameBytes
    $channelMs = $sw.Elapsed.TotalMilliseconds
    Write-Host "[Export] Get channel: $channelMs ms"

    $sw.Restart()
    try { $channel.WriteToPython($bytes, 5000) } catch {
        throw "Export-SharedMemoryVarBytes: Failed to write to shared memory channel: $_"
    }
    $writeMs = $sw.Elapsed.TotalMilliseconds
    Write-Host "[Export] WriteToPython: $writeMs ms"

    Write-Output $len
}

function Copy-VariableToSharedMemory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $ChannelName,
        [Parameter(Mandatory)] [int]    $FrameBytes,
        [Parameter(Mandatory)] [string] $VariableName,
        [string] $Encoding = 'utf8'
    )
    
    Write-Host "[Copy-VariableToSharedMemory] Variable: $VariableName → Channel: $ChannelName"
    
    # Resolve variable
    $val = Resolve-VariableValue -VariableName $VariableName
    
    # Fast path for byte arrays - no conversion needed!
    if ($val -is [byte[]]) {
        Write-Host "[Copy-VariableToSharedMemory] Direct byte[] - zero conversion"
        $bytes = $val
        $len = $bytes.Length
    }
    else {
        # Convert to bytes for other types
        Write-Host "[Copy-VariableToSharedMemory] Converting $($val.GetType()) to bytes"
        $bytes = Get-StrictBytes -InputObject $val -Encoding $Encoding
        $len = $bytes.Length
    }
    
    if ($len -gt $FrameBytes) {
        throw "Copy-VariableToSharedMemory: Variable size $len exceeds frame $FrameBytes"
    }
    
    Write-Host "[Copy-VariableToSharedMemory] Variable size: $len bytes"
    
    # Get or create channel (Python will have created it already)
    $channel = Get-OrCreateSharedMemoryChannel -ChannelName $ChannelName -FrameBytes $FrameBytes
    
    # Write directly to PS→PY region via DLL
    try {
        $channel.WriteToPython($bytes, 5000)
        Write-Host "[Copy-VariableToSharedMemory] Written to shared memory"
    } catch {
        throw "Copy-VariableToSharedMemory: Failed to write: $_"
    }
    
    # Return length for Python to know how much data to read
    Write-Output $len
}

# Writeable buffer class - PowerShell can write directly to shared memory
class WriteableBuffer {
    [string]$ChannelName
    [int]$Capacity
    hidden [SharedMemoryChannel]$_channel
    
    WriteableBuffer([string]$channelName, [int]$capacity, [SharedMemoryChannel]$channel) {
        $this.ChannelName = $channelName
        $this.Capacity = $capacity
        $this._channel = $channel
    }
    
    # Write byte array starting at offset 0 (simplified API)
    [void] WriteBytes([byte[]]$data) {
        $this.WriteBytes(0, $data)
    }
    
    # Write byte array at offset
    [void] WriteBytes([int]$offset, [byte[]]$data) {
        if ($offset -lt 0 -or ($offset + $data.Length) -gt $this.Capacity) {
            throw "WriteableBuffer: Write of $($data.Length) bytes at offset $offset exceeds capacity $($this.Capacity)"
        }
        
        # If writing full buffer from offset 0, use direct write
        if ($offset -eq 0) {
            Write-Host "[WriteableBuffer] Writing $($data.Length) bytes to shared memory..."
            $this._channel.WriteToPython($data, 5000)
            Write-Host "[WriteableBuffer] Wrote $($data.Length) bytes to shared memory"
        } else {
            throw "WriteableBuffer: Partial writes not yet implemented. Write from offset 0."
        }
    }
    
    # Fill entire buffer with data from scriptblock
    [void] FillFrom([scriptblock]$generator) {
        Write-Host "[WriteableBuffer] Filling buffer from generator..."
        $data = & $generator
        if ($data -isnot [byte[]]) {
            throw "WriteableBuffer: Generator must return byte[], got $($data.GetType())"
        }
        Write-Host "[WriteableBuffer] Generator returned $($data.Length) bytes"
        $this.WriteBytes(0, $data)
    }
    
    # Get current length written (from header)
    [int] GetLength() {
        return $this._channel.GetPowerShellLength()
    }
}

function New-SharedMemoryWriteableBuffer {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $ChannelName,
        [Parameter(Mandatory)] [int]    $FrameBytes,
        [Parameter(Mandatory)] [string] $VariableName
    )
    
    Write-Host "[New-SharedMemoryWriteableBuffer] Creating buffer: $VariableName (capacity: $FrameBytes bytes)"
    
    # Get or create channel
    $channel = Get-OrCreateSharedMemoryChannel -ChannelName $ChannelName -FrameBytes $FrameBytes
    
    # Create writeable buffer wrapper
    $buffer = [WriteableBuffer]::new($ChannelName, $FrameBytes, $channel)
    
    # Set as PowerShell variable
    $targetScope = 'Global'
    $targetName = $VariableName
    if ($VariableName -match '^(?<scope>global|script|private|local):(?<name>.+)$') {
        $targetScope = $Matches.scope
        $targetName = $Matches.name
    }
    
    Set-Variable -Name $targetName -Value $buffer -Scope $targetScope
    Write-Host "[New-SharedMemoryWriteableBuffer] Buffer created: $VariableName"
}
