param(
    [string]$Path = "./session.xml"
)

$sessionData = [ordered]@{}

# Exclude automatic variables that do not serialize or should not be restored
$excludedVariableNames = @(
    'Error', 'MyInvocation', 'PSBoundParameters', 'OutputEncoding',
    'PWD', 'PSItem', 'Matches', 'Args'
)

# Capture mutable global variables and their values
$sessionData.Variables = Get-Variable -Scope Global |
    Where-Object {
        $_.Options -notmatch 'ReadOnly|Constant' -and
        ($excludedVariableNames -notcontains $_.Name)
    } |
    ForEach-Object {
        [pscustomobject]@{
            Name  = $_.Name
            Value = $_.Value
        }
    }

# Capture user-defined functions so definitions can be restored
$sessionData.Functions = Get-ChildItem function:\ |
    Where-Object {
        $_.Options -notmatch 'ReadOnly|Constant' -and
        -not $_.Module
    } |
    ForEach-Object {
        [pscustomobject]@{
            Name       = $_.Name
            Definition = $_.Definition
            Options    = $_.Options.ToString()
        }
    }

# Capture alias mappings for rehydration
$sessionData.Aliases = Get-Alias |
    Where-Object { $_.Options -notmatch 'ReadOnly|Constant' } |
    ForEach-Object {
        [pscustomobject]@{
            Name      = $_.Name
            Definition = $_.Definition
            Options    = $_.Options.ToString()
        }
    }

# Track active modules so they can be re-imported
$sessionData.Modules = Get-Module |
    Where-Object { $_.Path } |
    ForEach-Object {
        [pscustomobject]@{
            Name    = $_.Name
            Version = $_.Version
        }
    }

# Preserve custom PSDrive mappings
$sessionData.Drives = Get-PSDrive |
    Where-Object { $_.IsNetwork -or $_.Provider.Name -ne 'FileSystem' -or $_.DisplayRoot } |
    ForEach-Object {
        [pscustomobject]@{
            Name     = $_.Name
            Provider = $_.Provider.Name
            Root     = $_.Root
            Description = $_.Description
        }
    }

# Save environment variables that might have been tweaked
$sessionData.Environment = Get-ChildItem Env: |
    ForEach-Object {
        [pscustomobject]@{
            Name  = $_.Name
            Value = $_.Value
        }
    }

# Record the current working directory and location stack
$sessionData.Location = [pscustomobject]@{
    Current = (Get-Location).Path
    Stack   = Get-Location -Stack | ForEach-Object { $_.Path }
}

# Persist recent history items for convenience
$sessionData.History = Get-History |
    ForEach-Object {
        [pscustomobject]@{
            Id          = $_.Id
            CommandLine = $_.CommandLine
        }
    }

$targetPath = $Path
if (-not [System.IO.Path]::IsPathRooted($targetPath)) {
    $targetPath = [System.IO.Path]::Combine((Get-Location).Path, $targetPath)
}

$targetDirectory = [System.IO.Path]::GetDirectoryName($targetPath)
if ($targetDirectory -and -not (Test-Path -LiteralPath $targetDirectory)) {
    New-Item -ItemType Directory -Path $targetDirectory -Force | Out-Null
}

Export-Clixml -InputObject $sessionData -Path $targetPath
