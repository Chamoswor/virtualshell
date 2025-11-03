# src/virtualshell/shared_memory_bridge.ps1

Set-StrictMode -Version Latest

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
    [int]$NumSlots = 1,
    [uint64]$Sequence = 0,
        [ValidateSet('Bytes','String','Json')] [string]$Format = 'Bytes',
        [string]$Encoding = 'utf8',
        [int]$DataLength = $FrameBytes,
        [string]$VariableName
    )

    if ($DataLength -lt 0) { throw "DataLength must be non-negative." }
    if ($DataLength -gt $FrameBytes) {
        throw "DataLength ($DataLength) cannot exceed FrameBytes ($FrameBytes)."
    }

    $channel = [SharedMemoryChannel]::new($ChannelName, $NumSlots, $FrameBytes)
    try {
    [byte[]]$raw = $channel.ReadFromPython($Sequence)
    if ($raw.Length -eq 0) { $raw = [byte[]]::new(0) }
        if ($DataLength -lt $raw.Length) {
            $trimmed = [byte[]]::new($DataLength)
            if ($DataLength -gt 0) {
                [System.Buffer]::BlockCopy($raw, 0, $trimmed, 0, $DataLength)
            }
            $raw = $trimmed
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
        }

        if ($PSBoundParameters.ContainsKey('VariableName')) {
            $targetScope = 'Global'
            $targetName = $VariableName
            if ($VariableName -match '^(?<scope>global|script|private|local):(?<name>.+)$') {
                $targetScope = $Matches.scope
                $targetName = $Matches.name
            }
            Set-Variable -Name $targetName -Value $value -Scope $targetScope
        }
    }
    finally {
        $channel.Dispose()
    }
}

function Export-SharedMemoryData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)] [string]$ChannelName,
        [Parameter(Mandatory = $true)] [int]$FrameBytes,
        [Parameter(Mandatory = $true)] [string]$Command,
    [int]$NumSlots = 1,
        [ValidateSet('Bytes','String','Json')] [string]$Format = 'Json',
        [string]$Encoding = 'utf8'
    )

    $scriptBlock = [ScriptBlock]::Create($Command)
    $result = & $scriptBlock

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
    }

    if ($bytes.Length -gt $FrameBytes) {
        throw "Payload length $($bytes.Length) exceeds frame capacity $FrameBytes."
    }

    if ($bytes.Length -lt $FrameBytes) {
    $buffer = [byte[]]::new($FrameBytes)
        if ($bytes.Length -gt 0) {
            [System.Buffer]::BlockCopy($bytes, 0, $buffer, 0, $bytes.Length)
        }
    } else {
        $buffer = $bytes
    }

    $channel = [SharedMemoryChannel]::new($ChannelName, $NumSlots, $FrameBytes)
    try {
        $channel.WriteToPython($buffer)
    }
    finally {
        $channel.Dispose()
    }

    Write-Output $bytes.Length
}
