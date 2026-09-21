# Type-graph reflection for generate_psobject(follow=True).
#
# Starting from one .NET type, walks the public instance surface (properties,
# fields, indexers, methods) and follows every referenced type that lives
# outside the .NET runtime / PowerShell install (i.e. the SDK the root type
# came from), so the generator can emit one Protocol per type and annotate
# members with each other. Runtime types (string, int, FileInfo, ...) are
# reported but not followed; enums are reported but not followed either
# (proxies surface them as strings).
#
# Works from the type alone: no live instance is needed, which matters for
# members that are unreachable at generation time (an empty collection's
# Item(index), a method that is expensive to call, ...).
#
# Output: one compact JSON object per type, one per line (NDJSON), so large
# graphs do not go through a single deep ConvertTo-Json.
#
# NOTE: dot-sourced into the user's session; must not touch preferences.

function Get-VsTypeGraph {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$RootType,

        # Only follow types in these namespaces (or namespaces nested under
        # them). Empty: every type outside the runtime is followed.
        [string[]]$IncludeNamespaces = @(),

        [int]$MaxTypes = 2000
    )

    # Variable names are case-insensitive: $psHome would collide with the read-only $PSHOME.
    $vsRuntimeDir = [System.Runtime.InteropServices.RuntimeEnvironment]::GetRuntimeDirectory().TrimEnd('\').ToLowerInvariant()
    $vsPsHome = ([string]$PSHOME).TrimEnd('\').ToLowerInvariant()

    function Get-AssemblyLocation([type]$t) {
        try { return [string]$t.Assembly.Location } catch { return '' }
    }

    function Test-External([type]$t) {
        $a = $null
        try { $a = $t.Assembly } catch { return $false }
        try { if ($a.GlobalAssemblyCache) { return $false } } catch { }
        $loc = Get-AssemblyLocation $t
        if (-not $loc) { return $false }
        $l = $loc.ToLowerInvariant()
        if ($vsRuntimeDir -and $l.StartsWith($vsRuntimeDir + '\')) { return $false }
        if ($vsPsHome -and $l.StartsWith($vsPsHome + '\')) { return $false }
        return $true
    }

    function Format-TypeName([type]$t) {
        if ($null -eq $t) { return 'System.Object' }
        if ($t.IsByRef) { $t = $t.GetElementType() }
        if ($t.IsArray) { return (Format-TypeName $t.GetElementType()) + '[]' }
        if ($t.IsGenericParameter) { return 'System.Object' }
        if ($t.IsGenericType) {
            $def = $t.GetGenericTypeDefinition().FullName
            if (-not $def) { $def = $t.Name }
            $_args = @($t.GetGenericArguments() | ForEach-Object { Format-TypeName $_ }) -join ','
            return "$def[$_args]"
        }
        $n = $t.FullName
        if (-not $n) { $n = $t.Name }
        return $n
    }

    $seen = @{}
    $queue = New-Object 'System.Collections.Generic.Queue[type]'

    function Add-Follow([type]$t) {
        if ($null -eq $t) { return }
        while ($t.IsByRef -or $t.IsArray) { $t = $t.GetElementType() }
        if ($t.IsGenericParameter) { return }
        if ($t.IsGenericType) {
            foreach ($arg in $t.GetGenericArguments()) { Add-Follow $arg }
            return
        }
        if ($t.IsPrimitive -or $t.IsPointer) { return }
        if (-not $t.FullName) { return }
        if ($seen.ContainsKey($t.FullName)) { return }
        if ($t.IsEnum) {
            # Reported (so the generator annotates it as str) but never walked.
            $seen[$t.FullName] = $true
            [pscustomobject]@{ tn = $t.FullName; isEnum = $true; ext = (Test-External $t) } |
                ConvertTo-Json -Compress
            return
        }
        if ($seen.Count -ge $MaxTypes) { return }
        if (-not (Test-External $t)) { return }
        if ($IncludeNamespaces.Count -gt 0) {
            $ns = [string]$t.Namespace
            $ok = $false
            foreach ($wanted in $IncludeNamespaces) {
                $wanted = $wanted.TrimEnd('.')
                if ($ns -eq $wanted -or $ns.StartsWith($wanted + '.')) { $ok = $true; break }
            }
            if (-not $ok) { return }
        }
        $seen[$t.FullName] = $true
        $queue.Enqueue($t)
    }

    $root = $RootType -as [type]
    if (-not $root) {
        throw "Type not found in this session: $RootType"
    }
    $seen[$root.FullName] = $true
    $queue.Enqueue($root)

    $flags = [System.Reflection.BindingFlags]'Public,Instance'

    while ($queue.Count -gt 0) {
        $t = $queue.Dequeue()
        $sources = @($t)
        if ($t.IsInterface) { $sources += $t.GetInterfaces() }

        $props = New-Object System.Collections.ArrayList
        $propNames = @{}
        $meths = @{}

        foreach ($src in $sources) {
            foreach ($p in $src.GetProperties($flags)) {
                $key = $p.Name + '|' + @($p.GetIndexParameters()).Count
                if ($propNames.ContainsKey($key)) { continue }
                $propNames[$key] = $true
                $index = @($p.GetIndexParameters() | ForEach-Object {
                    [pscustomobject]@{ n = $_.Name; t = (Format-TypeName $_.ParameterType) }
                })
                [void]$props.Add([pscustomobject]@{
                    n  = $p.Name
                    t  = (Format-TypeName $p.PropertyType)
                    w  = [bool]$p.CanWrite
                    ip = $index
                })
                Add-Follow $p.PropertyType
                foreach ($ipar in $p.GetIndexParameters()) { Add-Follow $ipar.ParameterType }
            }
            foreach ($f in $src.GetFields($flags)) {
                $key = $f.Name + '|0'
                if ($propNames.ContainsKey($key)) { continue }
                $propNames[$key] = $true
                [void]$props.Add([pscustomobject]@{
                    n  = $f.Name
                    t  = (Format-TypeName $f.FieldType)
                    w  = (-not $f.IsInitOnly)
                    ip = @()
                })
                Add-Follow $f.FieldType
            }
            foreach ($m in $src.GetMethods($flags)) {
                if ($m.IsSpecialName) { continue }   # property/event/operator accessors
                $overload = [pscustomobject]@{
                    r = (Format-TypeName $m.ReturnType)
                    p = @($m.GetParameters() | ForEach-Object {
                        [pscustomobject]@{ n = $_.Name; t = (Format-TypeName $_.ParameterType) }
                    })
                }
                if (-not $meths.ContainsKey($m.Name)) { $meths[$m.Name] = New-Object System.Collections.ArrayList }
                [void]$meths[$m.Name].Add($overload)
                Add-Follow $m.ReturnType
                foreach ($mp in $m.GetParameters()) { Add-Follow $mp.ParameterType }
            }
        }

        # Static members (declared on the type or inherited, but not
        # System.Object's Equals/ReferenceEquals). Instance members win on
        # name clashes. Proxies route these to [Type]::Member at runtime.
        $sflags = [System.Reflection.BindingFlags]'Public,Static,FlattenHierarchy'
        foreach ($p in $t.GetProperties($sflags)) {
            $key = $p.Name + '|' + @($p.GetIndexParameters()).Count
            if ($propNames.ContainsKey($key)) { continue }
            $propNames[$key] = $true
            [void]$props.Add([pscustomobject]@{
                n  = $p.Name
                t  = (Format-TypeName $p.PropertyType)
                w  = [bool]$p.CanWrite
                ip = @()
                s  = $true
            })
            Add-Follow $p.PropertyType
        }
        foreach ($f in $t.GetFields($sflags)) {
            $key = $f.Name + '|0'
            if ($propNames.ContainsKey($key)) { continue }
            $propNames[$key] = $true
            [void]$props.Add([pscustomobject]@{
                n  = $f.Name
                t  = (Format-TypeName $f.FieldType)
                w  = (-not ($f.IsInitOnly -or $f.IsLiteral))
                ip = @()
                s  = $true
            })
            Add-Follow $f.FieldType
        }
        $staticMeths = @{}
        foreach ($m in $t.GetMethods($sflags)) {
            if ($m.IsSpecialName -or $m.DeclaringType -eq [object]) { continue }
            if ($meths.ContainsKey($m.Name)) { continue }
            $overload = [pscustomobject]@{
                r = (Format-TypeName $m.ReturnType)
                p = @($m.GetParameters() | ForEach-Object {
                    [pscustomobject]@{ n = $_.Name; t = (Format-TypeName $_.ParameterType) }
                })
            }
            if (-not $staticMeths.ContainsKey($m.Name)) { $staticMeths[$m.Name] = New-Object System.Collections.ArrayList }
            [void]$staticMeths[$m.Name].Add($overload)
            Add-Follow $m.ReturnType
            foreach ($mp in $m.GetParameters()) { Add-Follow $mp.ParameterType }
        }

        $methodList = @($meths.Keys | Sort-Object | ForEach-Object {
            [pscustomobject]@{ n = $_; o = @($meths[$_]) }
        })
        $methodList += @($staticMeths.Keys | Sort-Object | ForEach-Object {
            [pscustomobject]@{ n = $_; o = @($staticMeths[$_]); s = $true }
        })

        [pscustomobject]@{
            tn      = $t.FullName
            asm     = (Get-AssemblyLocation $t)
            asmName = [string]$t.Assembly.FullName
            ext     = (Test-External $t)
            isEnum  = [bool]$t.IsEnum
            isIface = [bool]$t.IsInterface
            props   = @($props)
            meths   = $methodList
        } | ConvertTo-Json -Depth 6 -Compress
    }
}
