"""Live PowerShell object proxies (pure Python, cross-platform).

Replaces the former C++ PsProxy. A PsProxy binds to a PowerShell variable
and exposes its .NET/PowerShell members as Python attributes:

- scalar values (strings, numbers, bools, dates, enums, ...) travel inline
  as JSON over the command channel,
- byte[] payloads travel through the zero-copy bridge,
- complex values come back as sub-proxies bound to session variables,
  with `to_psobject()` available for a CliXml snapshot via the bridge.

Every read uses a runtime "discriminator" in PowerShell, so conversion is
based on the actual value, never on guessed schema types.
"""
from __future__ import annotations

import base64
import itertools
import json
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from .errors import ExecutionError
from .ps_object import PSObject

if TYPE_CHECKING:
    from .shell import Shell

__all__ = ["PsProxy"]


# =============================================================================
# Helpers
# =============================================================================

def _ps_quote(s: str) -> str:
    """PowerShell single-quoted literal (quotes doubled, no interpolation)."""
    return "'" + s.replace("'", "''") + "'"


_ASSEMBLY_HELPER = Path(__file__).resolve().parent / "assembly_resolver.ps1"


def helper_prelude(script: Path, function_name: str) -> List[str]:
    """Command lines that dot-source a bundled .ps1 unless `function_name` already exists.

    Prepend to a command so the helper is loaded in the same round trip.
    """
    lines: List[str] = []
    if os.name == "nt":
        # Dot-sourcing a .ps1 needs a permissive policy on Windows (5.1 defaults to Restricted).
        lines.append("Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force "
                     "-ErrorAction SilentlyContinue")
    lines.append(f"if (-not (Get-Command {function_name} -ErrorAction SilentlyContinue)) {{ . "
                 + _ps_quote(str(script)) + " }")
    return lines


def ensure_assembly_loaded(shell: "Shell", path: str, full_name: str = "") -> None:
    """Load the assembly at `path` into the session, once.

    Used for generated protocols that carry ``__ps_assembly__``: the type's
    assembly is not part of the runtime, so a fresh session has to load it
    before the creation expression can run. The assembly's directory is also
    registered with a process-wide AssemblyResolve probe (see
    assembly_resolver.ps1) so its own dependencies resolve. One round trip.
    """
    if not path:
        return
    if not shell.is_running:
        shell.start()
    lines = helper_prelude(_ASSEMBLY_HELPER, "Import-VsAssembly")
    lines.append(f"Import-VsAssembly -Path {_ps_quote(path)} -FullName {_ps_quote(full_name)}")
    res = shell.run("\n".join(lines))
    if not res.success:
        raise ExecutionError(
            f"Could not load assembly {path}: {(res.err or '').strip()[:400]}")


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VARIABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# A bare variable reference ("$tia", "$global:tia"): bound in place. Anything
# else starting with "$" ("$tia.Projects") is a derived expression that is
# evaluated once into a private variable.
_PLAIN_VAR_RE = re.compile(r"^\$(?:[A-Za-z]+:)?[A-Za-z_][A-Za-z0-9_]*$")
_TYPE_LIKE_RE = re.compile(r"^[A-Za-z0-9_\.\[\],`+ ]+$")

_counter = itertools.count()


def _next_var(prefix: str) -> str:
    return f"__vs_{prefix}_{next(_counter)}"


def _member_expr(ref: str, name: str) -> str:
    """Expression accessing property `name` on `ref` ($-qualified)."""
    if _IDENT_RE.match(name):
        return f"{ref}.{name}"
    return f"{ref}.PSObject.Properties[{_ps_quote(name)}].Value"


def _method_call_expr(ref: str, name: str, args: List[str]) -> str:
    arg_text = ", ".join(args)
    if _IDENT_RE.match(name):
        return f"{ref}.{name}({arg_text})"
    return f"{ref}.PSObject.Methods[{_ps_quote(name)}].Invoke({arg_text})"


# `ref` holds a [type] object in static mode; PowerShell resolves `::` against
# the contained type, so `$t = [Math]; $t::Sqrt(16)` works like `[Math]::Sqrt`.
_STATIC_BINDING_FLAGS = "[System.Reflection.BindingFlags]'Public,Static"


def _static_member_expr(ref: str, name: str) -> str:
    """Expression reading static member `name` on the type held by `ref`."""
    if _IDENT_RE.match(name):
        return f"{ref}::{name}"
    return (f"{ref}.InvokeMember({_ps_quote(name)}, "
            f"{_STATIC_BINDING_FLAGS},GetProperty,GetField', $null, $null, @())")


def _static_method_call_expr(ref: str, name: str, args: List[str]) -> str:
    arg_text = ", ".join(args)
    if _IDENT_RE.match(name):
        return f"{ref}::{name}({arg_text})"
    return (f"{ref}.InvokeMember({_ps_quote(name)}, "
            f"{_STATIC_BINDING_FLAGS},InvokeMethod', $null, $null, @({arg_text}))")


def static_type_literal(expr: str) -> Optional[str]:
    """Return the type name inside a bare ``[Type]`` literal, else None.

    A bare literal (nothing outside the brackets) is how callers name a
    static class, e.g. ``[System.Windows.Forms.MessageBox]``. Invocations
    (``[T]::new()``) and casts (``[int](3)``) do not match, and neither do
    unbalanced brackets, so generic literals like
    ``[System.Collections.Generic.List[int]]`` resolve correctly.
    """
    cleaned = (expr or "").strip()
    if len(cleaned) < 3 or not (cleaned.startswith("[") and cleaned.endswith("]")):
        return None
    inner = cleaned[1:-1].strip()
    if not inner or not _TYPE_LIKE_RE.fullmatch(inner):
        return None
    depth = 0
    for char in inner:
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth < 0:
                return None
    return inner if depth == 0 else None


def split_invocation(expr: str) -> Optional[Tuple[str, str]]:
    """Return (target, argument_text) when `expr` ends with a (...) call."""
    depth = 0
    start = -1
    for idx, char in enumerate(expr):
        if char == "(":
            if depth == 0:
                start = idx
            depth += 1
        elif char == ")":
            if depth == 0:
                return None
            depth -= 1
            if depth == 0 and start != -1 and idx == len(expr) - 1:
                head = expr[:start].strip()
                args = expr[start + 1:idx].strip()
                if head:
                    return head, args
    return None


def build_creation_strategies(raw: str) -> List[Tuple[str, str]]:
    """Candidate PowerShell expressions that materialise an object from `raw`.

    `raw` may be a variable name, a type name, or a constructor-like
    expression such as ``System.Text.StringBuilder(32)``. Each candidate
    evaluates to the object (callers wrap it in an assignment).
    """
    cleaned = raw.strip()
    strategies: List[Tuple[str, str]] = []
    seen: set = set()

    def add(label: str, command: str) -> None:
        text = command.strip()
        if text and text not in seen:
            seen.add(text)
            strategies.append((label, text))

    if not cleaned:
        return strategies

    if cleaned.startswith("$"):
        add("variable reference", cleaned)
        return strategies

    add("raw expression", cleaned)

    if _VARIABLE_NAME_RE.fullmatch(cleaned):
        add("Get-Variable lookup",
            f"Get-Variable -Name {cleaned} -ValueOnly -ErrorAction Stop")

    invocation = split_invocation(cleaned)
    call_type, call_args = invocation if invocation else (None, None)

    for candidate in filter(None, [call_type, cleaned]):
        cand = candidate.strip()
        if cand.startswith("[") and cand.endswith("]"):
            inner = cand[1:-1]
        else:
            inner = cand
        if not _TYPE_LIKE_RE.fullmatch(inner.replace("::", ".")):
            continue
        bracketed = f"[{inner}]"

        if call_args is not None and candidate == call_type:
            arglist = call_args.strip()
            ctor = f"({arglist})" if arglist else "()"
            add("static ::new (args)", f"{bracketed}::new{ctor}")
            if arglist:
                add("New-Object (with args)",
                    f"New-Object -TypeName {_ps_quote(inner)} -ArgumentList {arglist} -ErrorAction Stop")
            else:
                add("New-Object",
                    f"New-Object -TypeName {_ps_quote(inner)} -ErrorAction Stop")
        else:
            add("static ::new", f"{bracketed}::new()")
            add("New-Object",
                f"New-Object -TypeName {_ps_quote(inner)} -ErrorAction Stop")

        if "." in inner:
            add("COM object",
                f"New-Object -ComObject {_ps_quote(inner)} -ErrorAction Stop")

    return strategies


# =============================================================================
# Schema
# =============================================================================

@dataclass
class ParamMeta:
    name: str
    type_name: str
    is_out: bool = False
    is_by_ref: bool = False
    is_array: bool = False


@dataclass
class MethodOverload:
    return_type: str
    params: List[ParamMeta] = field(default_factory=list)


@dataclass
class MethodMeta:
    name: str
    overloads: List[MethodOverload] = field(default_factory=list)
    is_script_method: bool = False
    is_static: bool = False   # static .NET member, reached through an instance proxy

    def overload_for(self, arg_count: int) -> Optional[MethodOverload]:
        for ov in self.overloads:
            if len(ov.params) == arg_count:
                return ov
        return self.overloads[0] if self.overloads else None


@dataclass
class PropertyMeta:
    name: str
    type_name: str
    writable: bool = False
    is_static: bool = False


@dataclass
class Schema:
    type_name: str
    methods: Dict[str, MethodMeta] = field(default_factory=dict)
    properties: Dict[str, PropertyMeta] = field(default_factory=dict)


_SCHEMA_CACHE: Dict[Tuple[str, str], Schema] = {}
_SCHEMA_LOCK = threading.Lock()


# The single reflection round trip. {ref} is the $-qualified object variable.
_SCHEMA_SCRIPT = """
$__vs_o = {ref}
$__vs_props = @($__vs_o.PSObject.Properties | ForEach-Object {{
    [pscustomobject]@{{ n = $_.Name; t = [string]$_.TypeNameOfValue; w = $_.IsSettable }}
}})
$__vs_meths = @()
try {{
    $__vs_groups = $__vs_o.GetType().GetMethods() |
        Where-Object {{ -not $_.IsSpecialName -and -not $_.IsStatic }} |
        Group-Object Name
}} catch {{ $__vs_groups = @() }}
foreach ($__vs_g in $__vs_groups) {{
    $__vs_ovl = @(foreach ($__vs_mi in $__vs_g.Group) {{
        [pscustomobject]@{{
            r = $__vs_mi.ReturnType.FullName
            p = @(foreach ($__vs_pp in $__vs_mi.GetParameters()) {{
                [pscustomobject]@{{
                    n = $__vs_pp.Name
                    t = $__vs_pp.ParameterType.FullName
                    o = $__vs_pp.IsOut
                    rf = $__vs_pp.ParameterType.IsByRef
                    a = $__vs_pp.ParameterType.IsArray
                }}
            }})
        }}
    }})
    $__vs_meths += ,([pscustomobject]@{{ n = $__vs_g.Name; o = $__vs_ovl }})
}}
$__vs_sm = @($__vs_o.PSObject.Members |
    Where-Object {{ $_.MemberType -in @('ScriptMethod', 'ParameterizedProperty') }} |
    ForEach-Object {{ [pscustomobject]@{{ n = $_.Name }} }})
# Static members of the runtime type (inherited too, but not System.Object's
# Equals/ReferenceEquals): reachable on the instance proxy, Python style.
$__vs_sflags = [System.Reflection.BindingFlags]'Public,Static,FlattenHierarchy'
$__vs_sprops = @()
$__vs_smeths = @()
try {{
    $__vs_t = $__vs_o.GetType()
    foreach ($__vs_pi in $__vs_t.GetProperties($__vs_sflags)) {{
        $__vs_sprops += [pscustomobject]@{{
            n = $__vs_pi.Name
            t = [string]$__vs_pi.PropertyType.FullName
            w = ($null -ne $__vs_pi.SetMethod)
        }}
    }}
    foreach ($__vs_fi in $__vs_t.GetFields($__vs_sflags)) {{
        $__vs_sprops += [pscustomobject]@{{
            n = $__vs_fi.Name
            t = [string]$__vs_fi.FieldType.FullName
            w = (-not ($__vs_fi.IsInitOnly -or $__vs_fi.IsLiteral))
        }}
    }}
    $__vs_sgroups = $__vs_t.GetMethods($__vs_sflags) |
        Where-Object {{ -not $_.IsSpecialName -and $_.DeclaringType -ne [object] }} |
        Group-Object Name
    foreach ($__vs_g in $__vs_sgroups) {{
        $__vs_ovl = @(foreach ($__vs_mi in $__vs_g.Group) {{
            [pscustomobject]@{{
                r = $__vs_mi.ReturnType.FullName
                p = @(foreach ($__vs_pp in $__vs_mi.GetParameters()) {{
                    [pscustomobject]@{{
                        n = $__vs_pp.Name
                        t = $__vs_pp.ParameterType.FullName
                        o = $__vs_pp.IsOut
                        rf = $__vs_pp.ParameterType.IsByRef
                        a = $__vs_pp.ParameterType.IsArray
                    }}
                }})
            }}
        }})
        $__vs_smeths += ,([pscustomobject]@{{ n = $__vs_g.Name; o = $__vs_ovl }})
    }}
}} catch {{ }}
[pscustomobject]@{{
    tn = $__vs_o.PSObject.TypeNames[0]
    props = $__vs_props
    meths = $__vs_meths
    sm = $__vs_sm
    sprops = $__vs_sprops
    smeths = $__vs_smeths
}} | ConvertTo-Json -Depth 8 -Compress
""".strip()


# Static-mode counterpart: {ref} holds a [type] object, so reflection runs on
# the type itself (not GetType(), which would describe RuntimeType). Constants
# and static fields surface as properties; writability follows the setter /
# IsInitOnly / IsLiteral rules.
_STATIC_SCHEMA_SCRIPT = """
$__vs_o = {ref}
$__vs_flags = [System.Reflection.BindingFlags]'Public,Static'
$__vs_props = @()
foreach ($__vs_pi in $__vs_o.GetProperties($__vs_flags)) {{
    $__vs_props += [pscustomobject]@{{
        n = $__vs_pi.Name
        t = [string]$__vs_pi.PropertyType.FullName
        w = ($null -ne $__vs_pi.SetMethod)
    }}
}}
foreach ($__vs_fi in $__vs_o.GetFields($__vs_flags)) {{
    $__vs_props += [pscustomobject]@{{
        n = $__vs_fi.Name
        t = [string]$__vs_fi.FieldType.FullName
        w = (-not ($__vs_fi.IsInitOnly -or $__vs_fi.IsLiteral))
    }}
}}
$__vs_meths = @()
try {{
    $__vs_groups = $__vs_o.GetMethods($__vs_flags) |
        Where-Object {{ -not $_.IsSpecialName }} |
        Group-Object Name
}} catch {{ $__vs_groups = @() }}
foreach ($__vs_g in $__vs_groups) {{
    $__vs_ovl = @(foreach ($__vs_mi in $__vs_g.Group) {{
        [pscustomobject]@{{
            r = $__vs_mi.ReturnType.FullName
            p = @(foreach ($__vs_pp in $__vs_mi.GetParameters()) {{
                [pscustomobject]@{{
                    n = $__vs_pp.Name
                    t = $__vs_pp.ParameterType.FullName
                    o = $__vs_pp.IsOut
                    rf = $__vs_pp.ParameterType.IsByRef
                    a = $__vs_pp.ParameterType.IsArray
                }}
            }})
        }}
    }})
    $__vs_meths += ,([pscustomobject]@{{ n = $__vs_g.Name; o = $__vs_ovl }})
}}
[pscustomobject]@{{
    tn = [string]$__vs_o.FullName
    props = $__vs_props
    meths = $__vs_meths
    sm = @()
}} | ConvertTo-Json -Depth 8 -Compress
""".strip()


def _parse_schema(payload: Dict[str, Any]) -> Schema:
    schema = Schema(type_name=str(payload.get("tn") or "System.Object"))

    def as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    for prop in as_list(payload.get("props")):
        if not isinstance(prop, dict) or not prop.get("n"):
            continue
        name = str(prop["n"])
        schema.properties[name] = PropertyMeta(
            name=name,
            type_name=str(prop.get("t") or ""),
            writable=bool(prop.get("w")),
        )

    def parse_method(meth: Dict[str, Any], *, is_static: bool) -> MethodMeta:
        meta = MethodMeta(name=str(meth["n"]), is_static=is_static)
        for ov in as_list(meth.get("o")):
            if not isinstance(ov, dict):
                continue
            overload = MethodOverload(return_type=str(ov.get("r") or ""))
            for param in as_list(ov.get("p")):
                if not isinstance(param, dict):
                    continue
                overload.params.append(ParamMeta(
                    name=str(param.get("n") or ""),
                    type_name=str(param.get("t") or ""),
                    is_out=bool(param.get("o")),
                    is_by_ref=bool(param.get("rf")),
                    is_array=bool(param.get("a")),
                ))
            meta.overloads.append(overload)
        return meta

    for meth in as_list(payload.get("meths")):
        if not isinstance(meth, dict) or not meth.get("n"):
            continue
        schema.methods[str(meth["n"])] = parse_method(meth, is_static=False)

    for sm in as_list(payload.get("sm")):
        if isinstance(sm, dict) and sm.get("n"):
            name = str(sm["n"])
            schema.methods.setdefault(
                name, MethodMeta(name=name, is_script_method=True))

    # Static members: instance members win on name clashes.
    for prop in as_list(payload.get("sprops")):
        if not isinstance(prop, dict) or not prop.get("n"):
            continue
        name = str(prop["n"])
        if name in schema.properties or name in schema.methods:
            continue
        schema.properties[name] = PropertyMeta(
            name=name,
            type_name=str(prop.get("t") or ""),
            writable=bool(prop.get("w")),
            is_static=True,
        )
    for meth in as_list(payload.get("smeths")):
        if not isinstance(meth, dict) or not meth.get("n"):
            continue
        name = str(meth["n"])
        if name in schema.methods or name in schema.properties:
            continue
        schema.methods[name] = parse_method(meth, is_static=True)

    return schema


_AWAITABLE_RE = re.compile(r"System\.Threading\.Tasks\.(Task|ValueTask)")


# =============================================================================
# Value discrimination (runtime-typed reads)
# =============================================================================

# Applied after `$<ret> = <expr>` succeeded. Emits exactly one JSON line and
# cleans the temp variable up for every kind except 'o' (kept as a live
# reference for sub-proxies) and 'bytes' (fetched via the bridge first).
_DISCRIMINATOR = """
$__vs_v = ${ret}
if ($null -eq $__vs_v) {{
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    '{{"k":"null"}}'
}} elseif ($__vs_v -is [byte[]]) {{
    '{{"k":"bytes"}}'
}} elseif ($__vs_v -is [bool]) {{
    $__vs_r = @{{ k = 'b'; v = $__vs_v }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} elseif ($__vs_v -is [enum]) {{
    $__vs_r = @{{ k = 's'; v = $__vs_v.ToString() }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} elseif ($__vs_v -is [sbyte] -or $__vs_v -is [byte] -or $__vs_v -is [int16] -or $__vs_v -is [uint16] -or $__vs_v -is [int] -or $__vs_v -is [uint32] -or $__vs_v -is [long] -or $__vs_v -is [uint64] -or $__vs_v -is [bigint]) {{
    $__vs_r = @{{ k = 'i'; v = $__vs_v }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} elseif ($__vs_v -is [single] -or $__vs_v -is [double] -or $__vs_v -is [decimal]) {{
    $__vs_r = @{{ k = 'n'; v = [double]$__vs_v }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} elseif ($__vs_v -is [datetime]) {{
    $__vs_r = @{{ k = 'dt'; v = $__vs_v.ToString('o') }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} elseif ($__vs_v -is [timespan]) {{
    $__vs_r = @{{ k = 'ts'; v = $__vs_v.TotalSeconds }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} elseif ($__vs_v -is [string] -or $__vs_v -is [char] -or $__vs_v -is [guid] -or $__vs_v -is [version] -or $__vs_v -is [uri]) {{
    $__vs_r = @{{ k = 's'; v = [string]$__vs_v }} | ConvertTo-Json -Compress
    Remove-Variable -Name {ret} -ErrorAction SilentlyContinue
    $__vs_r
}} else {{
    @{{ k = 'o'; t = [string]$__vs_v.PSObject.TypeNames[0] }} | ConvertTo-Json -Compress
}}
""".strip()

# Find a generic method definition on {type_expr} (or its interfaces), close
# it over the named type arguments and keep the MethodInfo in {var}.
_GENERIC_LOOKUP = """
$__vs_gt = {type_expr}
$__vs_gf = [System.Reflection.BindingFlags]'Public,Instance,Static,FlattenHierarchy'
$__vs_gm = @($__vs_gt.GetMethods($__vs_gf) | Where-Object {{ $_.Name -eq {name} -and $_.IsGenericMethodDefinition -and $_.GetGenericArguments().Count -eq {ntypes} -and $_.GetParameters().Count -eq {nargs} }})
if ($__vs_gm.Count -eq 0) {{
    foreach ($__vs_gi in $__vs_gt.GetInterfaces()) {{
        $__vs_gm += @($__vs_gi.GetMethods() | Where-Object {{ $_.Name -eq {name} -and $_.IsGenericMethodDefinition -and $_.GetGenericArguments().Count -eq {ntypes} -and $_.GetParameters().Count -eq {nargs} }})
    }}
}}
if ($__vs_gm.Count -eq 0) {{ throw ("No generic method " + {name} + " with {ntypes} type parameter(s) and {nargs} argument(s) on " + $__vs_gt.FullName) }}
$__vs_ta = @()
foreach ($__vs_tn in @({types})) {{
    $__vs_tt = $__vs_tn -as [type]
    if (-not $__vs_tt) {{ throw ("Type argument not found in this session: " + $__vs_tn) }}
    $__vs_ta += $__vs_tt
}}
${var} = $__vs_gm[0].MakeGenericMethod([type[]]$__vs_ta)
""".strip()

# Bulk read: one row per element of {source}, one JSON document.
_SELECT_SCRIPT = """
$__vs_cell = {{
    param($v)
    if ($null -eq $v) {{ return $null }}
    if ($v -is [string] -or $v -is [bool] -or $v -is [sbyte] -or $v -is [byte] -or $v -is [int16] -or $v -is [uint16] -or $v -is [int] -or $v -is [uint32] -or $v -is [long] -or $v -is [uint64] -or $v -is [single] -or $v -is [double] -or $v -is [decimal]) {{ return $v }}
    if ($v -is [datetime]) {{ return $v.ToString('o') }}
    if ($v -is [enum] -or $v -is [char] -or $v -is [guid] -or $v -is [version] -or $v -is [uri] -or $v -is [timespan]) {{ return $v.ToString() }}
    return [string]$v
}}
$__vs_rows = @(foreach ($__vs_e in {source}) {{
    $__vs_row = [ordered]@{{}}
{cells}
    [pscustomobject]$__vs_row
}})
ConvertTo-Json -InputObject $__vs_rows -Depth 2 -Compress
""".strip()

_ISO_FRACTION_RE = re.compile(r"(\.\d{6})\d+")


def _parse_iso_datetime(text: str) -> datetime:
    # PowerShell round-trip format has 7 fractional digits; Python accepts 6.
    cleaned = _ISO_FRACTION_RE.sub(r"\1", text.replace("Z", "+00:00"))
    return datetime.fromisoformat(cleaned)


# =============================================================================
# The proxy
# =============================================================================

class PsProxy:
    """Python handle to a live PowerShell object.

    Attribute access reads/writes properties and calls methods on the
    underlying object inside the PowerShell session.

    A bare ``[Type]`` literal (or ``static=True``) selects static mode: the
    proxy binds the type itself and exposes its static methods, properties
    and constants, invoked with ``::`` in the session.
    """

    def __init__(
        self,
        shell: "Shell",
        type_name: str = "",
        object_ref: str = "$obj",
        *,
        timeout: Optional[float] = None,
        static: bool = False,
    ) -> None:
        object.__setattr__(self, "_shell", shell)
        object.__setattr__(self, "_timeout", timeout)
        object.__setattr__(self, "_dynamic", {})
        object.__setattr__(self, "_method_cache", {})

        if not shell.is_running:
            shell.start()

        ref = (object_ref or "").strip()
        if not ref:
            ref = type_name.strip()
        if not ref:
            raise ValueError("object_ref or type_name is required")

        literal = static_type_literal(ref)
        object.__setattr__(self, "_static", bool(static) or literal is not None)

        if _PLAIN_VAR_RE.fullmatch(ref):
            # Bind an existing variable in place.
            self._validate_ref(ref)
            object.__setattr__(self, "_ref", ref)
            object.__setattr__(self, "_owns_ref", False)
            object.__setattr__(self, "_origin", ref)
        elif self._static:
            inner = (literal if literal is not None else ref).strip()
            if inner.startswith("[") and inner.endswith("]"):
                inner = inner[1:-1].strip()
            object.__setattr__(self, "_ref", self._materialize_static(inner))
            object.__setattr__(self, "_owns_ref", True)
            object.__setattr__(self, "_origin", f"[{inner}]")
        else:
            # A creation expression, or a derived "$var.Member..." expression:
            # evaluated once into a private variable so later member access
            # and method calls do not re-run it.
            ref_var, origin = self._materialize(ref)
            object.__setattr__(self, "_ref", ref_var)
            object.__setattr__(self, "_owns_ref", True)
            object.__setattr__(self, "_origin", origin)
        self._register_origin()

        object.__setattr__(self, "_schema", self._load_schema(type_name))

    # -- infrastructure --------------------------------------------------------

    def _run(self, command: str, *, label: str) -> str:
        res = self._shell.run(command, timeout=self._timeout)
        if not res.success:
            raise ExecutionError(
                f"{label} failed: {(res.err or '').strip() or f'exit_code={res.exit_code}'}")
        return (res.out or "").strip()

    def _validate_ref(self, ref: str) -> None:
        if self._static:
            out = self._run(
                f"if ({ref} -is [type]) {{ 'ok' }} "
                f"elseif ($null -eq {ref}) {{ 'null' }} else {{ 'notatype' }}",
                label=f"Binding to {ref}")
            if out == "null":
                raise ValueError(f"PowerShell variable {ref} is null or undefined")
            if out != "ok":
                raise ValueError(
                    f"PowerShell variable {ref} does not hold a [type]; "
                    "a static proxy must bind a type object")
            return
        out = self._run(
            f"if ($null -eq {ref}) {{ 'null' }} else {{ 'ok' }}",
            label=f"Binding to {ref}")
        if out != "ok":
            hint = ""
            if ref.startswith("$__vs_"):
                hint = (" (a virtualshell temporary from another session; regenerate "
                        "the stub from the proxy object or pass expression=, so it "
                        "records how the object is reached, e.g. '$tia.Projects')")
            raise ValueError(f"PowerShell variable {ref} is null or undefined{hint}")

    def _materialize(self, expression: str) -> Tuple[str, str]:
        """Evaluate `expression` into a private variable.

        Returns ``($var, candidate)`` where `candidate` is the PowerShell
        expression that succeeded; it becomes the proxy's origin.
        """
        strategies = build_creation_strategies(expression)
        if not strategies:
            raise ValueError(f"Cannot build an object from {expression!r}")

        var = _next_var("proxy")
        errors: List[str] = []
        for label, candidate in strategies:
            res = self._shell.run(
                f"${var} = ({candidate})", timeout=self._timeout)
            if res.success:
                check = self._shell.run(
                    f"if ($null -eq ${var}) {{ 'null' }} else {{ 'ok' }}",
                    timeout=self._timeout)
                if check.success and (check.out or "").strip() == "ok":
                    return f"${var}", candidate
                errors.append(f"{label}: produced null")
            else:
                errors.append(f"{label}: {(res.err or '').strip()[:200]}")

        raise RuntimeError(
            f"Failed to create PowerShell object from {expression!r}. Tried:\n  "
            + "\n  ".join(errors))

    def _materialize_static(self, type_text: str) -> str:
        """Bind a ``[Type]`` literal to a session variable (static mode)."""
        inner = type_text.strip()
        if inner.startswith("[") and inner.endswith("]"):
            inner = inner[1:-1].strip()
        if not inner or not _TYPE_LIKE_RE.fullmatch(inner):
            raise ValueError(f"Cannot resolve a static type from {type_text!r}")

        var = _next_var("proxy")
        res = self._shell.run(f"${var} = [{inner}]", timeout=self._timeout)
        if res.success:
            check = self._shell.run(
                f"if (${var} -is [type]) {{ 'ok' }} else {{ 'no' }}",
                timeout=self._timeout)
            if check.success and (check.out or "").strip() == "ok":
                return f"${var}"
        raise RuntimeError(
            f"Failed to resolve static type [{inner}]: "
            f"{(res.err or '').strip()[:200] or 'expression did not yield a type'}")

    def _load_schema(self, provided_type: str) -> Schema:
        run_id = self._shell.python_run_id

        # A provided type name is only trusted as a cache key after we have
        # resolved it once; the real type name always comes from the object.
        script = _STATIC_SCHEMA_SCRIPT if self._static else _SCHEMA_SCRIPT
        out = self._run(
            script.format(ref=self._ref),
            label=f"Schema query for {self._ref}")
        try:
            payload = json.loads(out)
        except json.JSONDecodeError as exc:
            raise ExecutionError(
                f"Schema query for {self._ref} returned invalid JSON") from exc

        schema = _parse_schema(payload)
        # Static schemas get their own key: the same type can also have an
        # instance schema, and _sub_proxy looks instance schemas up by name.
        key = f"static:{schema.type_name}" if self._static else schema.type_name
        with _SCHEMA_LOCK:
            _SCHEMA_CACHE[(run_id, key)] = schema
        return schema

    @classmethod
    def _cached_schema(cls, run_id: str, type_name: str) -> Optional[Schema]:
        with _SCHEMA_LOCK:
            return _SCHEMA_CACHE.get((run_id, type_name))

    def _sub_proxy(self, ref: str, type_name: str,
                   origin: Optional[str] = None) -> "PsProxy":
        proxy = object.__new__(PsProxy)
        object.__setattr__(proxy, "_shell", self._shell)
        object.__setattr__(proxy, "_timeout", self._timeout)
        object.__setattr__(proxy, "_dynamic", {})
        object.__setattr__(proxy, "_method_cache", {})
        object.__setattr__(proxy, "_ref", ref)
        object.__setattr__(proxy, "_owns_ref", True)
        # Values returned from calls are instances, never types.
        object.__setattr__(proxy, "_static", False)
        object.__setattr__(proxy, "_origin", origin)
        proxy._register_origin()

        cached = self._cached_schema(self._shell.python_run_id, type_name)
        if cached is not None:
            object.__setattr__(proxy, "_schema", cached)
        else:
            object.__setattr__(proxy, "_schema", proxy._load_schema(type_name))
        return proxy

    # -- provenance ------------------------------------------------------------
    # Every proxy remembers the PowerShell expression it was reached by, so a
    # stub generated from a derived object ("$tia.Projects") can say how to get
    # there again instead of naming a temporary variable.

    def _register_origin(self) -> None:
        register = getattr(self._shell, "_register_proxy_origin", None)
        if register is not None and self._origin:
            register(self._ref, self._origin)

    def _static_base(self) -> str:
        """Expression that `::` binds against: the type for static proxies,
        the instance's runtime type otherwise."""
        return self._ref if self._static else f"({self._ref}.GetType())"

    def _origin_base(self, *, static: bool = False) -> Optional[str]:
        origin = self._origin
        if origin is None:
            return None
        if self._static:
            return origin
        if static:
            return f"({origin}.GetType())"
        if _PLAIN_VAR_RE.fullmatch(origin):
            return origin
        return f"({origin})"

    def _member_origin(self, name: str, *, static: bool = False) -> Optional[str]:
        base = self._origin_base(static=static)
        if base is None or not _IDENT_RE.match(name):
            return None
        return f"{base}::{name}" if (self._static or static) else f"{base}.{name}"

    def _call_origin(self, name: str, origin_args: List[Optional[str]],
                     *, awaited: bool, static: bool = False) -> Optional[str]:
        base = self._origin_base(static=static)
        if base is None or not _IDENT_RE.match(name) or any(a is None for a in origin_args):
            return None
        arg_text = ", ".join(a for a in origin_args if a is not None)
        if self._static or static:
            call = f"{base}::{name}({arg_text})"
        else:
            call = f"{base}.{name}({arg_text})"
        return f"({call}).GetAwaiter().GetResult()" if awaited else call

    def _origin_arg(self, value: Any, formatted: str) -> Optional[str]:
        """`formatted` if the argument can be re-created from source text, else None."""
        if isinstance(value, PsProxy):
            return value._origin
        if isinstance(value, (bytes, bytearray, memoryview)):
            return None  # travels through the bridge as a temporary variable
        if isinstance(value, (list, tuple, set)):
            items = list(value)
        elif isinstance(value, dict):
            items = list(value.values())
        else:
            return formatted
        for item in items:
            if isinstance(item, PsProxy) or self._origin_arg(item, "") is None:
                return None
        return formatted

    def _bridge(self):
        return self._shell.zero_copy_bridge()

    # -- value transport -------------------------------------------------------

    def _fetch_value(self, expr: str, *, label: str,
                     origin: Optional[str] = None,
                     prelude: Optional[List[str]] = None) -> Any:
        """Evaluate `expr` and convert the value by its runtime type.

        Assignment and the runtime-type read travel in ONE round trip: the
        assignment runs inside try/catch, and the reader only runs when it
        succeeded, so failures still surface as ExecutionError with the
        host's own error text. `prelude` lines run before the assignment in
        the same command (used by generic()). `origin` is the reproducible
        expression recorded on a resulting sub-proxy.
        """
        ret = _next_var("ret")
        assign = "\n".join(list(prelude or []) + [f"${ret} = ({expr})"])
        script = (
            "$__vs_ok = $false\n$__vs_err = $null\n"
            "try {\n" + assign + "\n$__vs_ok = $?\n} catch { $__vs_err = $_ }\n"
            "if ($__vs_ok) {\n" + _DISCRIMINATOR.format(ret=ret) + "\n}"
            " elseif ($null -ne $__vs_err) { Write-Error -ErrorRecord $__vs_err }"
            " else { Write-Error 'expression reported an error' }"
        )
        out = self._run(script, label=label)
        try:
            info = json.loads(out)
        except json.JSONDecodeError as exc:
            raise ExecutionError(f"{label}: unexpected reader output") from exc

        kind = info.get("k")
        if kind == "null":
            return None
        if kind == "b":
            return bool(info.get("v"))
        if kind == "i":
            return int(info.get("v"))
        if kind == "n":
            return float(info.get("v"))
        if kind == "s":
            return str(info.get("v"))
        if kind == "dt":
            return _parse_iso_datetime(str(info.get("v")))
        if kind == "ts":
            return timedelta(seconds=float(info.get("v")))
        if kind == "bytes":
            data = bytes(self._bridge().receive(f"${ret}",
                                                timeout=self._timeout or 30.0))
            self._shell.run(
                f"Remove-Variable -Name {ret} -ErrorAction SilentlyContinue")
            return data
        if kind == "o":
            return self._sub_proxy(f"${ret}", str(info.get("t") or ""), origin)
        raise ExecutionError(f"{label}: unknown value kind {kind!r}")

    def _format_argument(self, value: Any, *, cleanup: List[str]) -> str:
        if value is None:
            return "$null"
        if isinstance(value, PsProxy):
            return value._ref
        if isinstance(value, bool):
            return "$true" if value else "$false"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if value != value:
                return "[double]::NaN"
            if value == float("inf"):
                return "[double]::PositiveInfinity"
            if value == float("-inf"):
                return "[double]::NegativeInfinity"
            return repr(value)
        if isinstance(value, str):
            if "\r" in value or "\n" in value:
                # The stdin command stream normalizes line endings, so strings
                # with control characters travel as base64 instead of literals.
                b64 = base64.b64encode(value.encode("utf-8")).decode("ascii")
                return ("[System.Text.Encoding]::UTF8.GetString("
                        f"[System.Convert]::FromBase64String('{b64}'))")
            return _ps_quote(value)
        if isinstance(value, datetime):
            return (f"[datetime]::Parse({_ps_quote(value.isoformat())}, "
                    "[System.Globalization.CultureInfo]::InvariantCulture)")
        if isinstance(value, timedelta):
            return f"[timespan]::FromSeconds({value.total_seconds()!r})"
        if isinstance(value, (bytes, memoryview)):
            # Bulk-safe path: ship through the shared memory channel.
            var = _next_var("arg")
            self._bridge().send(bytes(value), var,
                                timeout=self._timeout or 30.0)
            cleanup.append(var)
            return f"${var}"
        if isinstance(value, (list, tuple, set)):
            items = ", ".join(
                self._format_argument(item, cleanup=cleanup) for item in value)
            return f"@({items})"
        if isinstance(value, dict):
            pairs = "; ".join(
                f"{_ps_quote(str(k))} = {self._format_argument(v, cleanup=cleanup)}"
                for k, v in value.items())
            return "@{" + pairs + "}"
        literal = getattr(value, "_ps_literal", None)
        if callable(literal):
            return str(literal())
        raise TypeError(
            f"Cannot marshal {type(value).__name__} to PowerShell")

    def _cleanup_vars(self, names: List[str]) -> None:
        if names:
            self._shell.run(
                "Remove-Variable -Name " + ",".join(names)
                + " -ErrorAction SilentlyContinue")

    # -- attribute protocol ----------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        schema: Schema = object.__getattribute__(self, "_schema")
        method_cache = object.__getattribute__(self, "_method_cache")
        dynamic = object.__getattribute__(self, "_dynamic")

        if name in method_cache:
            return method_cache[name]
        if name in schema.methods:
            bound = self._bind_method(schema.methods[name])
            method_cache[name] = bound
            return bound
        if name in schema.properties:
            meta = schema.properties[name]
            if self._static or meta.is_static:
                expr = _static_member_expr(self._static_base(), name)
            else:
                expr = _member_expr(self._ref, name)
            return self._fetch_value(
                expr, label=f"Read property {schema.type_name}.{name}",
                origin=self._member_origin(name, static=meta.is_static))
        if name in dynamic:
            return dynamic[name]
        raise AttributeError(
            f"{schema.type_name} proxy has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return

        schema: Schema = object.__getattribute__(self, "_schema")
        if name in schema.methods:
            raise AttributeError(f"Cannot overwrite proxied method {name!r}")
        if name in schema.properties:
            meta = schema.properties[name]
            if not meta.writable:
                raise AttributeError(f"Property {name!r} is read-only")
            static = self._static or meta.is_static
            if static and not _IDENT_RE.match(name):
                raise AttributeError(
                    f"Cannot write static member {name!r}: not an identifier")
            target = (_static_member_expr(self._static_base(), name) if static
                      else _member_expr(self._ref, name))
            cleanup: List[str] = []
            try:
                rhs = self._format_argument(value, cleanup=cleanup)
                self._run(
                    f"{target} = {rhs}",
                    label=f"Set property {schema.type_name}.{name}")
            finally:
                self._cleanup_vars(cleanup)
            return

        self._dynamic[name] = value

    # -- Python collection protocol --------------------------------------------
    # .NET collections behind a proxy behave like Python sequences / mappings:
    # len(), indexing (negative indices and slices included), iteration and
    # `in`. Driven by what the object actually exposes: Count/Length, an Item
    # indexer, GetEnumerator, and Keys/ContainsKey for dictionaries. Stubs
    # annotate such members as Sequence[T] / Mapping[K, V] accordingly.

    def _is_mapping_like(self) -> bool:
        s = self._schema
        return "Keys" in s.properties and "ContainsKey" in s.methods

    def _is_collection(self) -> bool:
        # Every IEnumerable (lists, arrays, dictionaries, sets, ...) has
        # GetEnumerator; a Count + Item indexer pair is collection-shaped too.
        # A StringBuilder (Length, Chars) is neither.
        s = self._schema
        return "GetEnumerator" in s.methods or ("Count" in s.properties and "Item" in s.methods)

    def _not_a_collection(self) -> TypeError:
        return TypeError(f"{self._schema.type_name} proxy is not a collection "
                         "(no IEnumerable, and no Count + Item indexer)")

    def __bool__(self) -> bool:
        return True  # a proxy always refers to an object; never len()-based

    def __len__(self) -> int:
        s = self._schema
        if not self._is_collection():
            raise self._not_a_collection()
        for prop in ("Count", "Length"):
            if prop in s.properties:
                return int(self._fetch_value(_member_expr(self._ref, prop),
                                             label=f"len({s.type_name})"))
        return int(self._run(f"@({self._ref}).Count", label=f"len({s.type_name})"))

    def __getitem__(self, index: Any) -> Any:
        s = self._schema
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        if not isinstance(index, int):
            if "Item" in s.methods:
                return self.Item(index)              # keyed indexer (dictionaries, ...)
            raise TypeError(f"{s.type_name} proxy has no keyed indexer")
        if not self._is_collection():
            raise self._not_a_collection()
        n = len(self)
        if index < 0:
            index += n
        if not 0 <= index < n:
            raise IndexError(f"{s.type_name} index out of range")
        if "Item" in s.methods:
            return self.Item(index)
        base = self._origin_base()
        return self._fetch_value(f"@({self._ref})[{index}]",
                                 label=f"{s.type_name}[{index}]",
                                 origin=f"@({base})[{index}]" if base else None)

    def __iter__(self):
        # Check eagerly (a generator body would only run on the first next()).
        if not self._is_collection():
            raise self._not_a_collection()
        return self._iterate()

    def _iterate(self):
        s = self._schema
        if self._is_mapping_like():
            source = f"@({self._ref}.Keys)"
        elif "Item" in s.methods and "Count" in s.properties:
            for i in range(len(self)):
                yield self.Item(i)
            return
        else:
            source = f"@({self._ref})"
        # Snapshot the enumeration once, then fetch element by element.
        arr = _next_var("iter")
        self._run(f"${arr} = {source}", label=f"iter({s.type_name})")
        try:
            count = int(self._run(f"${arr}.Count", label=f"iter({s.type_name})"))
            for i in range(count):
                yield self._fetch_value(f"${arr}[{i}]", label=f"iter({s.type_name})[{i}]")
        finally:
            self._cleanup_vars([arr])

    def __contains__(self, item: Any) -> bool:
        s = self._schema
        if self._is_mapping_like():
            return bool(self.ContainsKey(item))
        if "Contains" in s.methods:
            return bool(self.Contains(item))
        return any(element == item for element in self)

    def __dir__(self) -> List[str]:
        schema: Schema = object.__getattribute__(self, "_schema")
        names = {"type_name", "proxy_schema", "proxy_multi_call", "proxy_select",
                 "generic", "to_psobject", "ps_ref", "ps_origin", "release"}
        names.update(schema.methods)
        names.update(schema.properties)
        names.update(self._dynamic)
        return sorted(names)

    def __repr__(self) -> str:
        kind = "static type" if self._static else "type"
        return f"<PsProxy {kind}='{self._schema.type_name}' ref='{self._ref}'>"

    # -- methods ---------------------------------------------------------------

    def _bind_method(self, meta: MethodMeta) -> Callable[..., Any]:
        proxy = self

        def invoke(*args: Any, **kwargs: Any) -> Any:
            if kwargs:
                raise TypeError("Proxy methods do not support keyword arguments")
            return proxy._invoke_method(meta, list(args))

        invoke.__name__ = meta.name
        invoke.__qualname__ = f"{self._schema.type_name}.{meta.name}"
        invoke.__vs_proxy_method__ = meta.name  # type: ignore[attr-defined]
        return invoke

    def _invoke_method(self, meta: MethodMeta, args: List[Any]) -> Any:
        overload = meta.overload_for(len(args))
        cleanup: List[str] = []
        out_buffers: List[Tuple[int, str]] = []  # (python arg index, ps var)

        ps_args: List[str] = []
        origin_args: List[Optional[str]] = []
        try:
            for index, value in enumerate(args):
                param = None
                if overload is not None and index < len(overload.params):
                    param = overload.params[index]

                if isinstance(value, bytearray):
                    # Mutable buffer: allocate PS-side, copy back afterwards.
                    var = _next_var("buf")
                    size = len(value)
                    self._run(f"${var} = [byte[]]::new({size})",
                              label=f"Allocate buffer for {meta.name}")
                    cleanup.append(var)
                    out_buffers.append((index, var))
                    ps_args.append(f"${var}")
                    origin_args.append(None)
                elif param is not None and param.is_array and \
                        param.type_name.startswith("System.Byte") and \
                        isinstance(value, (bytes, memoryview)):
                    ps_args.append(self._format_argument(bytes(value),
                                                         cleanup=cleanup))
                    origin_args.append(None)
                else:
                    ps_args.append(self._format_argument(value,
                                                         cleanup=cleanup))
                    origin_args.append(self._origin_arg(value, ps_args[-1]))

            if self._static or meta.is_static:
                call = _static_method_call_expr(self._static_base(), meta.name, ps_args)
            else:
                call = _method_call_expr(self._ref, meta.name, ps_args)
            return_type = overload.return_type if overload else ""
            awaited = bool(_AWAITABLE_RE.search(return_type))
            if awaited:
                call = f"({call}).GetAwaiter().GetResult()"
            origin = (self._call_origin(meta.name, origin_args, awaited=awaited,
                                        static=meta.is_static)
                      if not out_buffers else None)

            label = f"Call {self._schema.type_name}.{meta.name}"
            if return_type in ("System.Void", "Void", "void"):
                self._run(call, label=label)
                result: Any = None
            else:
                result = self._fetch_value(call, label=label, origin=origin)

            for index, var in out_buffers:
                data = bytes(self._bridge().receive(
                    f"${var}", timeout=self._timeout or 30.0))
                target = args[index]
                target[:] = data

            return result
        finally:
            self._cleanup_vars(cleanup)

    # -- public API ------------------------------------------------------------

    @property
    def type_name(self) -> str:
        return self._schema.type_name

    @property
    def ps_ref(self) -> str:
        """The PowerShell variable this proxy is bound to (with $)."""
        return self._ref

    @property
    def ps_origin(self) -> Optional[str]:
        """PowerShell expression that reaches this object again, or None.

        ``"$tia"`` for a bound variable, ``"[T]::new(...)"`` for a created
        object, ``"$tia.Projects"`` / ``"$sb.Append('x')"`` for values obtained
        through a proxy. None when the path involved data that only existed
        as temporaries (bytes sent through the bridge, out-buffers).
        `generate_psobject` embeds it as ``__ps_expression__``.
        """
        return self._origin

    def proxy_schema(self) -> Dict[str, Any]:
        schema = self._schema
        return {
            "TypeName": schema.type_name,
            "Methods": [
                {"Name": m.name, "ScriptMethod": m.is_script_method,
                 "Overloads": len(m.overloads)}
                for m in schema.methods.values()
            ],
            "Properties": [
                {"Name": p.name, "TypeName": p.type_name, "Writable": p.writable}
                for p in schema.properties.values()
            ],
        }

    def proxy_multi_call(self, func: Callable[..., Any], *args: Any) -> List[Any]:
        """Invoke a proxied method many times in batched PowerShell commands.

        Accepts either a repeat count (`multi_call(m, 100)`) or a list of
        single arguments (`multi_call(m, [1, 2, 3])`). Results are returned
        as JSON-decoded values.
        """
        method_name = getattr(func, "__vs_proxy_method__", None)
        if method_name is None or method_name not in self._schema.methods:
            raise TypeError("Function is not a method of this proxy")

        if len(args) == 1 and isinstance(args[0], int):
            call_args: List[List[Any]] = [[] for _ in range(args[0])]
        elif len(args) == 1 and isinstance(args[0], (list, tuple)):
            call_args = [[item] for item in args[0]]
        else:
            call_args = [[item] for item in args]

        results: List[Any] = []
        batch_size = 500
        for start in range(0, len(call_args), batch_size):
            cleanup: List[str] = []
            try:
                lines = ["$__vs_mc = [System.Collections.Generic.List[object]]::new()"]
                for one_call in call_args[start:start + batch_size]:
                    ps_args = [self._format_argument(a, cleanup=cleanup)
                               for a in one_call]
                    if self._static or self._schema.methods[method_name].is_static:
                        call = _static_method_call_expr(
                            self._static_base(), method_name, ps_args)
                    else:
                        call = _method_call_expr(self._ref, method_name, ps_args)
                    lines.append(f"$__vs_mc.Add(({call}))")
                lines.append(
                    "ConvertTo-Json -InputObject $__vs_mc -Depth 4 -Compress")
                out = self._run("\n".join(lines),
                                label=f"multi_call {method_name}")
            finally:
                self._cleanup_vars(cleanup)

            if out:
                decoded = json.loads(out)
                if isinstance(decoded, list):
                    results.extend(decoded)
                else:
                    results.append(decoded)
        return results

    # -- generic methods (one round trip, MethodInfo cached per runtime type) --

    @staticmethod
    def _type_argument_name(value: Any) -> str:
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("[") and text.endswith("]"):
                text = text[1:-1].strip()
            if not text:
                raise ValueError("empty type argument")
            return text
        if isinstance(value, PsProxy):
            if not value._static:
                raise TypeError("type arguments must be types, not instances; "
                                "pass a static proxy, a generated class or a type name")
            return value._schema.type_name
        type_name = getattr(value, "__ps_type_name__", None)
        if type_name:
            return str(type_name)
        raise TypeError(f"Cannot use {value!r} as a .NET type argument")

    def generic(self, name: str, *type_args: Any) -> Callable[..., Any]:
        """Bind the generic method `name` closed over `type_args`.

        ``item.generic("GetService", SoftwareContainer)()`` runs
        ``GetService<SoftwareContainer>()`` on the object in ONE round trip:
        the method is found by reflection, closed with MakeGenericMethod and
        invoked inside a single PowerShell command (Windows PowerShell 5.1
        has no generic-call syntax). Type arguments may be generated
        protocol classes, static type proxies or .NET type names. The closed
        MethodInfo is cached in the session per (runtime type, name, type
        arguments, argument count), so later calls only Invoke().
        """
        if not _IDENT_RE.match(name or ""):
            raise ValueError(f"Invalid method name {name!r}")
        if not type_args:
            raise TypeError("generic() needs at least one type argument")
        type_names = [self._type_argument_name(t) for t in type_args]
        proxy = self

        def invoke(*args: Any) -> Any:
            return proxy._invoke_generic(name, type_names, list(args))

        invoke.__name__ = name
        invoke.__qualname__ = f"{self._schema.type_name}.{name}<{','.join(type_names)}>"
        return invoke

    def _invoke_generic(self, name: str, type_names: List[str], args: List[Any]) -> Any:
        cleanup: List[str] = []
        try:
            ps_args = [self._format_argument(a, cleanup=cleanup) for a in args]
            key = (self._schema.type_name, name, tuple(type_names), len(args))
            cache = getattr(self._shell, "_generic_methods", None)
            var = cache.get(key) if cache is not None else None
            prelude: List[str] = []
            if var is None:
                var = _next_var("gm")
                type_list = ", ".join(_ps_quote(t) for t in type_names)
                prelude = _GENERIC_LOOKUP.format(
                    var=var, type_expr=self._static_base(), name=_ps_quote(name),
                    types=type_list, ntypes=len(type_names), nargs=len(args)).splitlines()
            target = "$null" if self._static else self._ref
            arg_array = "[object[]]@(" + ", ".join(ps_args) + ")"
            call = (f"if (${var}.IsStatic) {{ ${var}.Invoke($null, {arg_array}) }} "
                    f"else {{ ${var}.Invoke({target}, {arg_array}) }}")
            label = f"Call {self._schema.type_name}.{name}<{','.join(type_names)}>"
            result = self._fetch_value(call, label=label, prelude=prelude)
            if cache is not None:
                cache[key] = var
            return result
        finally:
            self._cleanup_vars(cleanup)

    # -- bulk reads --------------------------------------------------------------

    def proxy_select(self, *expressions: str, **aliases: str) -> List[Dict[str, Any]]:
        """Read several members of every element in ONE round trip.

        ``blocks.proxy_select("Name", "GetType().FullName")`` returns
        ``[{"Name": ..., "GetType().FullName": ...}, ...]``; keyword form
        names the columns: ``proxy_select(name="Name", type="GetType().FullName")``.
        Each expression is evaluated as ``$element.<expression>``. Scalars
        come back as Python values (datetimes as ISO strings, enums as their
        names); anything else as its string form. On a non-collection the
        result has one row. Use this for read-only listings; use live proxies
        when you need to act on the objects.
        """
        columns: List[Tuple[str, str]] = [(e, e) for e in expressions]
        columns += [(k, v) for k, v in aliases.items()]
        if not columns:
            raise ValueError("proxy_select() needs at least one expression")
        for key, expr in columns:
            if not expr or "\n" in expr or "\r" in expr:
                raise ValueError(f"Invalid expression for column {key!r}")
        cells = "\n".join(
            f"        $__vs_row[{_ps_quote(key)}] = (& $__vs_cell ($__vs_e.{expr}))"
            for key, expr in columns)
        source = f"@({self._ref}.Values)" if self._is_mapping_like() else f"@({self._ref})"
        script = _SELECT_SCRIPT.format(source=source, cells=cells)
        out = self._run(script, label=f"proxy_select on {self._schema.type_name}")
        if not out:
            return []
        try:
            rows = json.loads(out)
        except json.JSONDecodeError as exc:
            raise ExecutionError("proxy_select: unexpected output") from exc
        if isinstance(rows, dict):
            rows = [rows]
        return [dict(r) for r in rows if isinstance(r, dict)]

    def to_psobject(self, *, depth: int = 2, timeout: float = 30.0) -> PSObject:
        """Snapshot the live object as a PSObject via the zero-copy bridge."""
        data = self._bridge().receive(self._ref, timeout=timeout, depth=depth)
        return PSObject.from_bytes(bytes(data))

    def release(self) -> None:
        """Remove the proxy's session variable (only if the proxy created it)."""
        if self._owns_ref and self._shell.is_running:
            name = self._ref.lstrip("$")
            self._shell.run(
                f"Remove-Variable -Name {name} -ErrorAction SilentlyContinue")
