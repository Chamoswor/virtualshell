"""Tool-schema generation from PowerShell command metadata.

``Shell.command_schema("Get-Process")`` and ``Shell.module_schemas("Pester")``
turn ``Get-Command`` metadata into MCP-style tool definitions: JSON Schema for
the parameters, ``enum`` values from ``[ValidateSet]`` and enum types,
required flags from ``Mandatory``, and the synopsis from ``Get-Help``. An
agent (or a human) can then call an unfamiliar vendor module correctly on the
first try instead of probing it.

The result is plain dicts shaped like MCP tool definitions::

    {
      "name": "Get-Process",
      "description": "Gets the processes that are running ...",
      "inputSchema": {
        "type": "object",
        "properties": {
          "Name": {"type": "array", "items": {"type": "string"}, ...},
          ...
        },
        "required": [...]
      },
      "x-ps-module": "Microsoft.PowerShell.Management",
      "x-ps-output-type": ["Process"]
    }
"""
from __future__ import annotations

import json
import secrets
from typing import Any, Dict, List, Optional, Tuple

from ._util import quote_pwsh_literal
from .errors import ExecutionError, VirtualShellError

# .NET type -> (JSON Schema type, format). Anything absent maps to "string";
# the original .NET type is always preserved in "x-ps-type".
_TYPE_MAP: Dict[str, Tuple[str, Optional[str]]] = {
    "System.String": ("string", None),
    "System.Char": ("string", None),
    "System.Boolean": ("boolean", None),
    "System.Management.Automation.SwitchParameter": ("boolean", None),
    "System.Byte": ("integer", None),
    "System.SByte": ("integer", None),
    "System.Int16": ("integer", None),
    "System.UInt16": ("integer", None),
    "System.Int32": ("integer", None),
    "System.UInt32": ("integer", None),
    "System.Int64": ("integer", None),
    "System.UInt64": ("integer", None),
    "System.Single": ("number", None),
    "System.Double": ("number", None),
    "System.Decimal": ("number", None),
    "System.DateTime": ("string", "date-time"),
    "System.TimeSpan": ("string", "duration"),
    "System.Guid": ("string", "uuid"),
    "System.Uri": ("string", "uri"),
}

# PowerShell's common parameters, excluded from generated schemas.
_COMMON_PARAMETERS = (
    "Verbose", "Debug", "ErrorAction", "WarningAction", "InformationAction",
    "ErrorVariable", "WarningVariable", "InformationVariable", "OutVariable",
    "OutBuffer", "PipelineVariable", "ProgressAction", "WhatIf", "Confirm",
)

_SCHEMA_TEMPLATE = """\
$__vs_err = $null
$__vs_cmds = @()
try {{
{lookup}
}} catch {{ $__vs_err = ($_ | Out-String).Trim() }}
$__vs_common = @({common})
$__vs_list = @(foreach ($__vs_c in $__vs_cmds) {{
    if ($__vs_c.CommandType -eq 'Alias') {{ $__vs_c = $__vs_c.ResolvedCommand }}
    if (-not $__vs_c) {{ continue }}
    $__vs_params = @(foreach ($__vs_p in $__vs_c.Parameters.Values) {{
        if ($__vs_common -contains $__vs_p.Name) {{ continue }}
        $__vs_pa = $null; $__vs_vs = $null; $__vs_mand = $false
        foreach ($__vs_at in $__vs_p.Attributes) {{
            if ($__vs_at -is [System.Management.Automation.ParameterAttribute]) {{
                if (-not $__vs_pa) {{ $__vs_pa = $__vs_at }}
                if ($__vs_at.Mandatory) {{ $__vs_mand = $true }}
            }}
            if ($__vs_at -is [System.Management.Automation.ValidateSetAttribute] -and -not $__vs_vs) {{ $__vs_vs = $__vs_at }}
        }}
        $__vs_enum = $null
        if ($__vs_vs) {{ $__vs_enum = @($__vs_vs.ValidValues) }}
        elseif ($__vs_p.ParameterType -and $__vs_p.ParameterType.IsEnum) {{ $__vs_enum = @([System.Enum]::GetNames($__vs_p.ParameterType)) }}
        [pscustomobject]@{{
            name = $__vs_p.Name
            type = [string]$__vs_p.ParameterType.FullName
            mandatory = $__vs_mand
            enum = $__vs_enum
            aliases = @($__vs_p.Aliases)
            position = $(if ($__vs_pa) {{ $__vs_pa.Position }} else {{ -2147483648 }})
            pipeline = [bool]($__vs_pa -and ($__vs_pa.ValueFromPipeline -or $__vs_pa.ValueFromPipelineByPropertyName))
            help = $(if ($__vs_pa -and $__vs_pa.HelpMessage) {{ [string]$__vs_pa.HelpMessage }} else {{ $null }})
        }}
    }})
    $__vs_syn = $null
{help}
    [pscustomobject]@{{
        name = $__vs_c.Name
        module = [string]$__vs_c.ModuleName
        synopsis = $__vs_syn
        outputType = @($__vs_c.OutputType | ForEach-Object {{ [string]$_.Name }})
        parameters = $__vs_params
    }}
}})
$__vs_json = ConvertTo-Json -InputObject ([pscustomobject]@{{ ok = ($null -eq $__vs_err); error = $__vs_err; data = $__vs_list }}) -Depth 8 -Compress -WarningAction SilentlyContinue
[Console]::Out.WriteLine('{beg}')
[Console]::Out.WriteLine($__vs_json)
[Console]::Out.WriteLine('{end}')"""

_HELP_SNIPPET = """\
    try {
        $__vs_h = Get-Help -Name $__vs_c.Name -ErrorAction Stop
        if ($__vs_h.Synopsis) { $__vs_syn = ([string]$__vs_h.Synopsis).Trim() }
    } catch { }"""


def build_schema_script(*, command: Optional[str] = None,
                        module: Optional[str] = None,
                        include_help: bool = True) -> Tuple[str, str, str]:
    """Compose the PowerShell snippet that describes `command` or every
    exported command of `module`. Returns ``(script, begin, end)`` markers."""
    if (command is None) == (module is None):
        raise ValueError("pass exactly one of command= or module=")
    if command is not None:
        lookup = (f"    $__vs_cmds = @(Get-Command -Name {quote_pwsh_literal(command)} "
                  "-ErrorAction Stop | Select-Object -First 1)")
    else:
        m = quote_pwsh_literal(module or "")
        lookup = (f"    Import-Module -Name {m} -ErrorAction Stop | Out-Null\n"
                  f"    $__vs_cmds = @(Get-Command -Module {m} "
                  "-CommandType Cmdlet,Function -ErrorAction Stop)")
    token = secrets.token_hex(4)
    beg, end = f"<<VSSCH_{token}>>", f"<<VSSCH_END_{token}>>"
    common = ",".join(quote_pwsh_literal(p) for p in _COMMON_PARAMETERS)
    script = _SCHEMA_TEMPLATE.format(
        lookup=lookup, common=common,
        help=_HELP_SNIPPET if include_help else "",
        beg=beg, end=end)
    return script, beg, end


def _property_schema(param: Dict[str, Any]) -> Dict[str, Any]:
    """Map one extracted parameter description to a JSON Schema property."""
    ps_type = str(param.get("type") or "")
    is_array = ps_type.endswith("[]")
    element = ps_type[:-2] if is_array else ps_type
    json_type, fmt = _TYPE_MAP.get(element, ("string", None))

    inner: Dict[str, Any] = {"type": json_type}
    if fmt:
        inner["format"] = fmt
    enum = param.get("enum")
    if enum:
        inner["enum"] = [str(v) for v in enum]
        inner["type"] = "string" if json_type == "boolean" else inner["type"]

    prop: Dict[str, Any] = {"type": "array", "items": inner} if is_array else inner
    if param.get("help"):
        prop["description"] = str(param["help"])
    prop["x-ps-type"] = ps_type
    position = param.get("position")
    if isinstance(position, int) and position >= 0:
        prop["x-ps-position"] = position
    if param.get("pipeline"):
        prop["x-ps-pipeline"] = True
    aliases = [a for a in (param.get("aliases") or []) if a]
    if aliases:
        prop["x-ps-aliases"] = [str(a) for a in aliases]
    return prop


def _tool_schema(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Map one extracted command description to an MCP-style tool dict."""
    properties: Dict[str, Any] = {}
    required: List[str] = []
    for param in entry.get("parameters") or []:
        name = str(param.get("name") or "")
        if not name:
            continue
        properties[name] = _property_schema(param)
        if param.get("mandatory"):
            required.append(name)

    name = str(entry.get("name") or "")
    schema: Dict[str, Any] = {
        "name": name,
        "description": str(entry.get("synopsis") or f"PowerShell command {name}"),
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": sorted(required),
        },
    }
    if entry.get("module"):
        schema["x-ps-module"] = str(entry["module"])
    output_type = [str(t) for t in (entry.get("outputType") or []) if t]
    if output_type:
        schema["x-ps-output-type"] = output_type
    return schema


def parse_schema_output(out: str, beg: str, end: str) -> List[Dict[str, Any]]:
    """Parse the schema snippet's stdout into a list of tool dicts."""
    start, stop = out.find(beg), out.find(end)
    if start == -1 or stop == -1 or stop < start:
        raise VirtualShellError(
            "command_schema: PowerShell did not return a result envelope")
    segment = out[start + len(beg):stop].strip()
    try:
        envelope = json.loads(segment) if segment else {}
    except ValueError as e:
        raise VirtualShellError(
            f"command_schema: could not parse the JSON envelope: {e}") from e
    if not envelope.get("ok", False):
        raise ExecutionError(str(envelope.get("error") or "command lookup failed"))
    data = envelope.get("data")
    if data is None:
        data = []
    elif not isinstance(data, list):
        data = [data]
    return [_tool_schema(entry) for entry in data if isinstance(entry, dict)]
