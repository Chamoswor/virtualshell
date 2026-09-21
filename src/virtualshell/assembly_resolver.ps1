# Assembly loading helpers used by Shell.make_proxy for generated protocol
# classes whose type lives outside the .NET runtime and the PowerShell
# install (generate_psobject records such assemblies as __ps_assembly__).
#
# [Reflection.Assembly]::LoadFrom is not always enough for those: the loaded
# assembly's own dependencies are resolved by name, and when they are not in
# the GAC or next to the host they are not found (or, as with TIA Openness,
# found but torn down badly). A process-wide AssemblyResolve handler that
# probes registered directories fixes that.
#
# The handler is written in C# rather than as a PowerShell script block on
# purpose: a script block delegate re-enters the engine on every resolve
# request, and the cmdlets it runs trigger further resolve requests, which
# recurses until the stack overflows (observed under Windows PowerShell 5.1).
# The C# handler carries a re-entrancy guard and does no engine work.
#
# NOTE: dot-sourced into the user's session; must not touch preferences.

function Register-VsAssemblyDirectory {
    <#
    .SYNOPSIS
        Add a directory to the process-wide dependency probe list.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if (-not ('VirtualShell.AssemblyDirResolver' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace VirtualShell {
    public static class AssemblyDirResolver {
        static readonly object _lock = new object();
        static readonly List<string> _dirs = new List<string>();
        static bool _installed;
        [ThreadStatic] static bool _busy;

        public static void AddDirectory(string dir) {
            lock (_lock) {
                if (!_dirs.Contains(dir)) _dirs.Add(dir);
                if (!_installed) {
                    AppDomain.CurrentDomain.AssemblyResolve += Handler;
                    _installed = true;
                }
            }
        }

        public static string[] Directories() {
            lock (_lock) { return _dirs.ToArray(); }
        }

        static Assembly Handler(object sender, ResolveEventArgs args) {
            if (_busy) return null;          // LoadFrom below may raise a nested resolve
            _busy = true;
            try {
                string name = new AssemblyName(args.Name).Name;
                foreach (string dir in Directories()) {
                    string path = Path.Combine(dir, name + ".dll");
                    if (File.Exists(path)) return Assembly.LoadFrom(path);
                }
                return null;
            } finally {
                _busy = false;
            }
        }
    }
}
'@
    }

    [VirtualShell.AssemblyDirResolver]::AddDirectory($Path)
}

function Import-VsAssembly {
    <#
    .SYNOPSIS
        Load an assembly from disk once, with its directory registered for
        dependency probing.
    .PARAMETER Path
        Path to the .dll.
    .PARAMETER FullName
        Optional assembly full name; when an assembly with this name is
        already loaded nothing is done.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path,

        [string]$FullName = ''
    )

    if ($FullName) {
        $loaded = [AppDomain]::CurrentDomain.GetAssemblies() |
            Where-Object { $_.FullName -eq $FullName } |
            Select-Object -First 1
        if ($loaded) {
            return
        }
    }

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Assembly not found: $Path"
    }
    $full = (Resolve-Path -LiteralPath $Path).ProviderPath
    Register-VsAssemblyDirectory -Path (Split-Path -Parent $full)
    [System.Reflection.Assembly]::LoadFrom($full) | Out-Null
}
