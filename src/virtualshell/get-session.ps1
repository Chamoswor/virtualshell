param(
	[string]$Path = "./session.xml"
)

if (-not (Test-Path -LiteralPath $Path)) {
	Write-Warning "Session file '$Path' was not found."
	return
}

try {
	$sessionData = Import-Clixml -Path $Path
} catch {
	Write-Error "Unable to import session data: $_"
	return
}

function ConvertTo-ScopedOptions {
	param($Value)
	if ($Value -is [System.Management.Automation.ScopedItemOptions]) {
		return $Value
	}
	$stringValue = [string]$Value
	if (-not $stringValue -or $stringValue -eq "None") {
		return [System.Management.Automation.ScopedItemOptions]::None
	}
	try {
		return [System.Enum]::Parse([System.Management.Automation.ScopedItemOptions], $stringValue)
	} catch {
		Write-Warning "Could not parse scoped options value '$stringValue'."
		return [System.Management.Automation.ScopedItemOptions]::None
	}
}

if ($sessionData.Variables) {
	foreach ($variable in $sessionData.Variables) {
		try {
			Set-Variable -Name $variable.Name -Value $variable.Value -Scope Global -Force
		} catch {
			Write-Warning "Failed to restore variable '$($variable.Name)': $_"
		}
	}
}

if ($sessionData.Functions) {
	foreach ($function in $sessionData.Functions) {
		try {
			$functionPath = "function:global:$($function.Name)"
			Remove-Item -Path $functionPath -ErrorAction SilentlyContinue
			Set-Item -Path $functionPath -Value $function.Definition -Force
			$options = ConvertTo-ScopedOptions -Value $function.Options
			if ($options -ne [System.Management.Automation.ScopedItemOptions]::None) {
				Set-Item -Path $functionPath -Options $options
			}
		} catch {
			Write-Warning "Failed to restore function '$($function.Name)': $_"
		}
	}
}

if ($sessionData.Aliases) {
	foreach ($alias in $sessionData.Aliases) {
		try {
			$options = ConvertTo-ScopedOptions -Value $alias.Options
			Set-Alias -Name $alias.Name -Value $alias.Definition -Scope Global -Option $options -Force
		} catch {
			Write-Warning "Failed to restore alias '$($alias.Name)': $_"
		}
	}
}

if ($sessionData.Modules) {
	foreach ($module in $sessionData.Modules) {
		try {
			if ($module.Version) {
				Import-Module -Name $module.Name -RequiredVersion $module.Version -ErrorAction Stop | Out-Null
			} else {
				Import-Module -Name $module.Name -ErrorAction Stop | Out-Null
			}
		} catch {
			Write-Warning "Failed to import module '$($module.Name)': $_"
		}
	}
}

if ($sessionData.Drives) {
	foreach ($drive in $sessionData.Drives) {
		if (Get-PSDrive -Name $drive.Name -ErrorAction SilentlyContinue) {
			continue
		}
		try {
			New-PSDrive -Name $drive.Name -PSProvider $drive.Provider -Root $drive.Root -Description $drive.Description -Scope Global | Out-Null
		} catch {
			Write-Warning "Failed to recreate PSDrive '$($drive.Name)': $_"
		}
	}
}

if ($sessionData.Environment) {
	foreach ($envVar in $sessionData.Environment) {
		try {
			Set-Item -Path "Env:$($envVar.Name)" -Value $envVar.Value -Force
		} catch {
			Write-Warning "Failed to restore environment variable '$($envVar.Name)': $_"
		}
	}
}

if ($sessionData.Location) {
	$stackPaths = @()
	if ($sessionData.Location.Stack) {
		$stackPaths = @($sessionData.Location.Stack) | Where-Object { $_ }
	}
	if ($stackPaths.Count -gt 0) {
		[array]::Reverse($stackPaths)
		foreach ($stackPath in $stackPaths) {
			try {
				Push-Location -Path $stackPath
			} catch {
				Write-Warning "Failed to restore stack path '$stackPath': $_"
			}
		}
	}
	if ($sessionData.Location.Current) {
		try {
			Set-Location -Path $sessionData.Location.Current
		} catch {
			Write-Warning "Failed to restore working location '$($sessionData.Location.Current)': $_"
		}
	}
}

if ($sessionData.History) {
	$psrlType = [Type]::GetType("Microsoft.PowerShell.PSConsoleReadLine, Microsoft.PowerShell.PSReadLine2", $false)
	if (-not $psrlType) {
		$psrlType = [Type]::GetType("Microsoft.PowerShell.PSConsoleReadLine, Microsoft.PowerShell.PSReadLine", $false)
	}
	foreach ($entry in $sessionData.History) {
		try {
			$command = [string]$entry.CommandLine
			if ([string]::IsNullOrWhiteSpace($command)) {
				continue
			}
			if ($psrlType -and $psrlType.GetMethod("AddToHistory", [Type[]]@([string]))) {
				$psrlType::AddToHistory($command)
			} else {
				Write-Warning "PSConsoleReadLine not available; skipping history entry '$command'."
			}
		} catch {
			$cmd = if ($entry.CommandLine) { $entry.CommandLine } else { '<unknown>' }
			Write-Warning "Failed to restore history entry '$cmd': $_"
		}
	}
}
