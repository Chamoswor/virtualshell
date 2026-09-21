from __future__ import annotations

import json
import keyword
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Set, Tuple



DEFAULT_COMMAND = "Get-Process | Select-Object -First 1"
DEFAULT_OUTPUT = "dot.py"

TYPE_MAP: Dict[str, Tuple[str, Tuple[str, ...], Tuple[str, ...]]] = {
    "System.String": ("str", tuple(), tuple()),
    "System.Int32": ("int", tuple(), tuple()),
    "System.Int64": ("int", tuple(), tuple()),
    "System.UInt32": ("int", tuple(), tuple()),
    "System.UInt64": ("int", tuple(), tuple()),
    "System.Double": ("float", tuple(), tuple()),
    "System.Single": ("float", tuple(), tuple()),
    "System.Decimal": ("float", tuple(), tuple()),
    "System.Boolean": ("bool", tuple(), tuple()),
    "System.Guid": ("str", tuple(), tuple()),
    "System.Byte": ("int", tuple(), tuple()),
    "System.SByte": ("int", tuple(), tuple()),
    "System.Int16": ("int", tuple(), tuple()),
    "System.UInt16": ("int", tuple(), tuple()),
    "System.Char": ("str", tuple(), tuple()),
    "byte": ("int", tuple(), tuple()),
    "sbyte": ("int", tuple(), tuple()),
    "short": ("int", tuple(), tuple()),
    "ushort": ("int", tuple(), tuple()),
    "uint": ("int", tuple(), tuple()),
    "ulong": ("int", tuple(), tuple()),
    "System.DateTime": ("datetime.datetime", tuple(), ("datetime",)),
    "System.TimeSpan": ("datetime.timedelta", tuple(), ("datetime",)),
    "System.Version": ("str", tuple(), tuple()),
    "System.Uri": ("str", tuple(), tuple()),
    "string": ("str", tuple(), tuple()),
    "String": ("str", tuple(), tuple()),
    "char": ("str", tuple(), tuple()),
    "int": ("int", tuple(), tuple()),
    "Int32": ("int", tuple(), tuple()),
    "Int64": ("int", tuple(), tuple()),
    "long": ("int", tuple(), tuple()),
    "double": ("float", tuple(), tuple()),
    "float": ("float", tuple(), tuple()),
    "bool": ("bool", tuple(), tuple()),
    "void": ("None", tuple(), tuple()),
    "System.Void": ("None", tuple(), tuple()),
    "object": ("Any", ("Any",), tuple()),
    "System.Object": ("Any", ("Any",), tuple()),
    "datetime": ("datetime.datetime", tuple(), ("datetime",)),
    "timespan": ("datetime.timedelta", tuple(), ("datetime",)),
    "guid": ("str", tuple(), tuple()),
    "version": ("str", tuple(), tuple()),
    "uri": ("str", tuple(), tuple()),
    "decimal": ("float", tuple(), tuple()),
}

METHOD_PATTERN = re.compile(r"(?P<ret>[^\s]+)\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\((?P<params>.*)\)")
ARRAY_PATTERN = re.compile(r"(?P<inner>.+)\[\]")
NULLABLE_PATTERN = re.compile(r"System\.Nullable`1\[(?P<inner>.+)\]")
GENERIC_PATTERN = re.compile(r"System\.Collections\.Generic\.(?P<outer>\w+)`\d+\[(?P<inner>.+)\]")
GENERIC_COLLECTIONS: Dict[str, str] = {
    "List": "List",
    "IList": "List",
    "IEnumerable": "List",
    "ICollection": "List",
    "Collection": "List",
    "Dictionary": "Dict",
    "IDictionary": "Dict",
    "HashSet": "Set",
    "ISet": "Set",
}

VARIABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TYPE_LIKE_PATTERN = re.compile(r"^[A-Za-z0-9_\.\[\],`+]+$")
SETTER_TOKEN_RE = re.compile(r"set\s*(?:;|\(|=)", re.IGNORECASE)


def split_generic_arguments(text: str) -> List[str]:
    args: List[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(text[start:index].strip())
            start = index + 1
    args.append(text[start:].strip())
    return [arg for arg in args if arg]


def first_signature(definition: str) -> str:
    depth = 0
    for index, char in enumerate(definition):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            return definition[:index]
    return definition


def sanitize_identifier(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name)
    if not cleaned:
        cleaned = "member"
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    if keyword.iskeyword(cleaned):
        cleaned = f"{cleaned}_"
    return cleaned


def map_ps_type(type_name: str) -> Tuple[str, Set[str], Set[str]]:
    typing_bits: Set[str] = set()
    runtime_bits: Set[str] = set()

    if not type_name:
        typing_bits.add("Any")
        return "Any", typing_bits, runtime_bits

    name = type_name.strip()

    nullable_match = NULLABLE_PATTERN.fullmatch(name)
    if nullable_match:
        inner = nullable_match.group("inner")
        ann, t_bits, r_bits = map_ps_type(inner)
        typing_bits.update(t_bits)
        runtime_bits.update(r_bits)
        typing_bits.add("Optional")
        return f"Optional[{ann}]", typing_bits, runtime_bits

    generic_match = GENERIC_PATTERN.fullmatch(name)
    if generic_match:
        outer = generic_match.group("outer")
        inner_raw = generic_match.group("inner")
        args = split_generic_arguments(inner_raw)
        target = GENERIC_COLLECTIONS.get(outer)

        if target == "Dict":
            key_ann, key_t, key_r = map_ps_type(args[0] if args else "")
            val_ann, val_t, val_r = map_ps_type(args[1] if len(args) > 1 else "")
            typing_bits.update(key_t)
            typing_bits.update(val_t)
            runtime_bits.update(key_r)
            runtime_bits.update(val_r)
            typing_bits.add("Dict")
            return f"Dict[{key_ann}, {val_ann}]", typing_bits, runtime_bits

        inner_ann, inner_t, inner_r = map_ps_type(args[0] if args else "")
        typing_bits.update(inner_t)
        runtime_bits.update(inner_r)

        if target == "Set":
            typing_bits.add("Set")
            return f"Set[{inner_ann}]", typing_bits, runtime_bits
        if target == "List":
            typing_bits.add("List")
            return f"List[{inner_ann}]", typing_bits, runtime_bits

        typing_bits.add("Any")
        return "Any", typing_bits, runtime_bits

    # char[] binds from a plain string in PowerShell (the binder converts),
    # and Python has no char type - so `str` is the accurate annotation.
    # Mapping it to List[str] would wrongly suggest lists of words are valid.
    if name in {"char[]", "Char[]", "System.Char[]"}:
        return "str", typing_bits, runtime_bits

    array_match = ARRAY_PATTERN.fullmatch(name)
    if array_match:
        inner = array_match.group("inner")
        ann, t_bits, r_bits = map_ps_type(inner)
        typing_bits.update(t_bits)
        runtime_bits.update(r_bits)
        typing_bits.add("List")
        return f"List[{ann}]", typing_bits, runtime_bits

    base = TYPE_MAP.get(name)
    if not base:
        simple = name.split(".")[-1]
        base = TYPE_MAP.get(simple)

    if base:
        annotation, typing_extra, runtime_extra = base
        typing_bits.update(typing_extra)
        runtime_bits.update(runtime_extra)
        if annotation == "Any":
            typing_bits.add("Any")
        if annotation.startswith("Optional["):
            typing_bits.add("Optional")
        return annotation, typing_bits, runtime_bits

    typing_bits.add("Any")
    return "Any", typing_bits, runtime_bits


def _split_top_level(text: str, separator: str = ",") -> List[str]:
    """Split on `separator` while ignoring separators inside []/() nesting."""
    parts: List[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char in "[(":
            depth += 1
        elif char in "])":
            depth = max(0, depth - 1)
        elif char == separator and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return [part.strip() for part in parts if part.strip()]


def parse_parameters(raw: str) -> List[Tuple[str, str, Set[str], Set[str]]]:
    if not raw.strip():
        return []
    result: List[Tuple[str, str, Set[str], Set[str]]] = []
    used_names: Set[str] = set()
    for index, segment in enumerate(_split_top_level(raw)):
        # Split "type name" on the LAST top-level space so array/generic
        # types (e.g. "byte[] buffer", "Dictionary`2[string,int] map") keep
        # their full type text.
        depth = 0
        split_at = -1
        for pos, char in enumerate(segment):
            if char in "[(":
                depth += 1
            elif char in "])":
                depth = max(0, depth - 1)
            elif char == " " and depth == 0:
                split_at = pos
        if split_at == -1:
            param_type = segment
            param_name = f"arg{index}"
        else:
            param_type = segment[:split_at].strip()
            param_name = segment[split_at + 1:].strip() or f"arg{index}"
        safe_name = sanitize_identifier(param_name)
        if safe_name == "self":
            safe_name = "self_"
        if safe_name in used_names:
            safe_name = f"{safe_name}_{index}"
        used_names.add(safe_name)
        annotation, typing_bits, runtime_bits = map_ps_type(param_type)
        result.append((safe_name, annotation, typing_bits, runtime_bits))
    return result


def split_signatures(definition: str) -> List[str]:
    """Split a Get-Member Definition into its individual overload signatures.

    Overloads are joined with ", " at the top level; commas inside parameter
    lists `()` and generic brackets `[]` must not split.
    """
    parts: List[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(definition):
        if char in "([":
            depth += 1
        elif char in ")]":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            parts.append(definition[start:index])
            start = index + 1
    parts.append(definition[start:])
    return [part.strip() for part in parts if part.strip()]


def build_method_signatures(name: str, entry: MutableMapping[str, Any], typing_bits: Set[str], runtime_bits: Set[str]) -> List[str]:
    """Render stub lines for a method, one `@overload` per distinct signature.

    .NET methods are overloaded rather than having optional parameters, so a
    call like ``sb.Append("x")`` only type-checks when every overload arity
    is present in the generated Protocol.
    """
    definitions: List[str] = []
    raw: Any = entry.get("Definition")
    if isinstance(raw, list):
        definitions.extend(item for item in raw if isinstance(item, str))
    elif isinstance(raw, str):
        definitions.append(raw)
    raw = entry.get("OverloadDefinitions")
    if isinstance(raw, list):
        definitions.extend(item for item in raw if isinstance(item, str))

    # (param annotations) -> (return annotation, parameters); first return wins
    # so char/string overloads that both map to `str` collapse into one stub.
    signatures: "OrderedDict[Tuple[str, ...], Tuple[str, List[Tuple[str, str, Set[str], Set[str]]]]]" = OrderedDict()
    for text in definitions:
        for sig_text in split_signatures(text):
            # Static members are rendered as "static <ret> Name(...)".
            sig_text = re.sub(r"^\s*static\s+", "", sig_text)
            match = METHOD_PATTERN.match(sig_text)
            if not match:
                continue
            return_ann, extra_typing, extra_runtime = map_ps_type(match.group("ret"))
            parameters = parse_parameters(match.group("params"))
            key = tuple(annotation for _, annotation, _, _ in parameters)
            if key in signatures:
                continue
            typing_bits.update(extra_typing)
            runtime_bits.update(extra_runtime)
            for _, _, t_bits, r_bits in parameters:
                typing_bits.update(t_bits)
                runtime_bits.update(r_bits)
            signatures[key] = (return_ann, parameters)

    def render(return_ann: str, parameters: List[Tuple[str, str, Set[str], Set[str]]]) -> str:
        params_text = ", ".join(f"{param}: {annotation}"
                                for param, annotation, _, _ in parameters)
        if params_text:
            return f"    def {name}(self, {params_text}) -> {return_ann}: ..."
        return f"    def {name}(self) -> {return_ann}: ..."

    if not signatures:
        typing_bits.add("Any")
        return [f"    def {name}(self, *args: Any, **kwargs: Any) -> Any: ..."]

    if len(signatures) == 1:
        return_ann, parameters = next(iter(signatures.values()))
        return [render(return_ann, parameters)]

    typing_bits.update({"Any", "overload"})
    lines: List[str] = []
    for return_ann, parameters in signatures.values():
        lines.append("    @overload")
        lines.append(render(return_ann, parameters))
    # Catch-all implementation so the @overload group is valid in a .py file.
    lines.append(f"    def {name}(self, *args: Any, **kwargs: Any) -> Any: ...")
    return lines


# PSMemberTypes values: AliasProperty=1, CodeProperty=2, Property=4,
# NoteProperty=8, ScriptProperty=16, PropertySet=32.
PROPERTY_FLAGS = {1, 2, 4, 8, 16, 32}
# Method=64, CodeMethod=128, ScriptMethod=256, ParameterizedProperty=512.
# Parameterized properties (indexers like StringBuilder.Chars) take arguments
# and are invoked with method syntax in PowerShell, so they render as methods.
METHOD_FLAGS = {64, 128, 256, 512}
NOTE_PROPERTY_FLAGS = {1, 8}  # AliasProperty / NoteProperty: always writable


def categorize_members(members: Iterable[MutableMapping[str, Any]]) -> Dict[str, OrderedDict[str, MutableMapping[str, Any]]]:
    methods: OrderedDict[str, MutableMapping[str, Any]] = OrderedDict()
    properties: OrderedDict[str, MutableMapping[str, Any]] = OrderedDict()

    for entry in members:
        name = entry.get("Name")
        if not isinstance(name, str):
            continue
        member_type = entry.get("MemberType")

        if isinstance(member_type, int):
            if member_type in PROPERTY_FLAGS:
                properties.setdefault(name, entry)
            elif member_type in METHOD_FLAGS:
                methods.setdefault(name, entry)
            continue

        member_type_text = str(member_type or "")
        if "Method" in member_type_text or member_type_text == "ParameterizedProperty":
            methods.setdefault(name, entry)
        elif "Property" in member_type_text:
            properties.setdefault(name, entry)

    return {"Methods": methods, "Properties": properties}


def property_is_writable(entry: MutableMapping[str, Any]) -> bool:
    setter = entry.get("SetMethod")
    if setter not in (None, "", False):
        return True

    definition = entry.get("Definition")
    texts: List[str] = []
    if isinstance(definition, list):
        texts.extend(str(item) for item in definition if isinstance(item, str))
    elif isinstance(definition, str):
        texts.append(definition)

    for text in texts:
        if SETTER_TOKEN_RE.search(text):
            return True

    member_type = entry.get("MemberType")
    if isinstance(member_type, str) and member_type in {"NoteProperty", "AliasProperty"}:
        return True
    if isinstance(member_type, int) and member_type in NOTE_PROPERTY_FLAGS:
        return True

    return False


def _property_type(entry: MutableMapping[str, Any]) -> str:
    """Best-effort property type: TypeNameOfValue when present, else the
    leading type token of the Get-Member Definition ("int Year {get;}")."""
    explicit = entry.get("TypeNameOfValue")
    if isinstance(explicit, str) and explicit:
        return explicit

    definition = entry.get("Definition")
    if isinstance(definition, list) and definition:
        definition = definition[0]
    if isinstance(definition, str):
        # Static definitions read "static datetime Now {get;}".
        text = re.sub(r"^\s*static\s+", "", definition.strip())
        token = text.split(" ", 1)[0]
        if token and TYPE_LIKE_PATTERN.fullmatch(token):
            return token
    return ""


def render_protocol(class_name: str, members: Iterable[MutableMapping[str, Any]], *,
                    ps_type_name: str = "", ps_expression: str = "",
                    ps_static: bool = False,
                    ps_assembly: str = "", ps_assembly_name: str = "") -> str:
    grouped = categorize_members(members)
    typing_bits: Set[str] = {"Protocol"}
    runtime_bits: Set[str] = set()

    prop_lines: List[str] = []
    for original_name, entry in grouped["Properties"].items():
        safe_name = sanitize_identifier(original_name)
        annotation, t_bits, r_bits = map_ps_type(_property_type(entry))
        typing_bits.update(t_bits)
        runtime_bits.update(r_bits)
        prop_lines.append("    @property")
        prop_lines.append(f"    def {safe_name}(self) -> {annotation}: ...")
        if property_is_writable(entry):
            prop_lines.append(f"    @{safe_name}.setter")
            prop_lines.append(f"    def {safe_name}(self, value: {annotation}) -> None: ...")
        prop_lines.append("")

    method_lines: List[str] = []
    seen_method_names: Set[str] = set()
    for original_name, entry in grouped["Methods"].items():
        safe_name = sanitize_identifier(original_name)
        if safe_name in seen_method_names:
            continue
        seen_method_names.add(safe_name)
        stub_lines = build_method_signatures(safe_name, entry, typing_bits, runtime_bits)
        method_lines.extend(stub_lines)
        if len(stub_lines) > 1:
            method_lines.append("")  # visual separation after overload groups

    if method_lines and method_lines[-1] == "":
        method_lines.pop()
    method_lines.append("    def proxy_multi_call(self, func: Callable[..., Any], *args: Any) -> List[Any]: ...")
    method_lines.append("    def proxy_schema(self) -> Dict[str, Any]: ...")
    method_lines.append("")
    method_lines.append("    @property")
    method_lines.append("    def type_name(self) -> str: ...")
    method_lines.append("")
    method_lines.append("    @property")
    method_lines.append("    def ps_ref(self) -> str: ...")
    method_lines.append("")
    method_lines.append("    @property")
    method_lines.append("    def ps_origin(self) -> str: ...")
    typing_bits.update({"Any", "List", "Dict", "Callable"})

    if prop_lines and prop_lines[-1] == "":
        prop_lines.pop()

    # Embedded metadata lets `Shell.make_proxy(GeneratedProtocol)` recreate
    # the object without the caller repeating the PowerShell expression.
    meta_lines: List[str] = []
    if ps_type_name:
        meta_lines.append(f"    __ps_type_name__: ClassVar[str] = {ps_type_name!r}")
    if ps_expression:
        meta_lines.append(f"    __ps_expression__: ClassVar[str] = {ps_expression!r}")
    if ps_static:
        meta_lines.append("    __ps_static__: ClassVar[bool] = True")
    if ps_assembly:
        # Outside the runtime: make_proxy loads this before the expression runs.
        meta_lines.append(f"    __ps_assembly__: ClassVar[str] = {ps_assembly!r}")
        if ps_assembly_name:
            meta_lines.append(f"    __ps_assembly_name__: ClassVar[str] = {ps_assembly_name!r}")
    if meta_lines:
        typing_bits.add("ClassVar")

    lines: List[str] = []
    lines.append("# This file was generated by virtualshell.generate_psobject")
    lines.append("from __future__ import annotations")
    lines.append("")

    if runtime_bits:
        for module_name in sorted(runtime_bits):
            lines.append(f"import {module_name}")
        lines.append("")

    typing_names = sorted(typing_bits)
    lines.append(f"from typing import {', '.join(typing_names)}")
    lines.append("")
    lines.append(f"__all__ = ['{class_name}']")
    lines.append("")
    lines.append(f"class {class_name}(Protocol):")

    if meta_lines:
        lines.extend(meta_lines)
        lines.append("")

    if not prop_lines and not method_lines:
        if not meta_lines:
            lines.append("    ...")
    else:
        lines.extend(prop_lines)
        if prop_lines and method_lines:
            lines.append("")
        lines.extend(method_lines)

    lines.append("")
    return "\n".join(lines)


def _decode_members(raw_text: str) -> List[MutableMapping[str, Any]]:
    if not raw_text:
        raise RuntimeError("Get-Member returned no data")
    try:
        members: Any = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Failed to parse Get-Member output as JSON") from exc

    if isinstance(members, MutableMapping):
        members = [members]
    if not members:
        raise RuntimeError("Get-Member produced an empty result set")
    return members


def fetch_members(shell, command: str) -> Tuple[str, List[MutableMapping[str, Any]]]:
    shell.run("Remove-Variable obj -ErrorAction SilentlyContinue", raise_on_error=False)
    assignment = f"$obj = ({command})"
    shell.run(assignment, raise_on_error=True)

    type_result = shell.run("$obj.PSObject.TypeNames[0]", raise_on_error=True)
    type_name = (type_result.out or "").strip()
    if not type_name:
        raise RuntimeError("Object reports no type name")

    # -InputObject (instead of piping) so collections report their own
    # members rather than the members of their elements.
    raw_result = shell.run(
        "Get-Member -InputObject $obj | ConvertTo-Json -Depth 6 -Compress",
        raise_on_error=True,
    )
    return type_name, _decode_members((raw_result.out or "").strip())


def fetch_static_members(shell, type_text: str) -> Tuple[str, List[MutableMapping[str, Any]]]:
    """Static members of ``[type_text]``.

    Piping (not -InputObject) matters here: Get-Member special-cases a piped
    System.Type with -Static and reports that type's own static members.
    """
    shell.run("Remove-Variable obj -ErrorAction SilentlyContinue", raise_on_error=False)
    shell.run(f"$obj = [{type_text}]", raise_on_error=True)

    type_result = shell.run("[string]$obj.FullName", raise_on_error=True)
    type_name = (type_result.out or "").strip()
    if not type_name:
        raise RuntimeError("Type reports no name")

    raw_result = shell.run(
        "$obj | Get-Member -Static | ConvertTo-Json -Depth 6 -Compress",
        raise_on_error=True,
    )
    return type_name, _decode_members((raw_result.out or "").strip())


# Where does the type come from, and can it be built without arguments?
# {type_expr} is `$obj.GetType()` for instances and `$obj` for static types.
_ORIGIN_SCRIPT = """
$__vs_t = {type_expr}
$__vs_a = $__vs_t.Assembly
$__vs_loc = ''
try {{ $__vs_loc = [string]$__vs_a.Location }} catch {{ }}
$__vs_gac = $false
try {{ $__vs_gac = [bool]$__vs_a.GlobalAssemblyCache }} catch {{ }}
[pscustomobject]@{{
    loc = $__vs_loc
    name = [string]$__vs_a.FullName
    gac = $__vs_gac
    rt = [string][System.Runtime.InteropServices.RuntimeEnvironment]::GetRuntimeDirectory()
    pshome = [string]$PSHOME
    tn = [string]$__vs_t.FullName
    ctor0 = ($null -ne $__vs_t.GetConstructor([Type[]]@()))
}} | ConvertTo-Json -Compress
""".strip()

_PLAIN_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def fetch_origin(shell, *, static: bool = False) -> Dict[str, Any]:
    """Assembly and constructor facts about ``$obj`` (empty dict on failure)."""
    script = _ORIGIN_SCRIPT.format(type_expr="$obj" if static else "$obj.GetType()")
    res = shell.run(script, raise_on_error=False)
    if not res.success:
        return {}
    try:
        data = json.loads((res.out or "").strip())
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _under(path: str, root: str) -> bool:
    if not root:
        return False
    norm = lambda p: p.replace("/", "\\").lower().rstrip("\\")  # noqa: E731
    return norm(path).startswith(norm(root) + "\\")


def is_external_assembly(origin: MutableMapping[str, Any]) -> bool:
    """True when the type's assembly must be loaded explicitly in a new session.

    Runtime assemblies (GAC, the .NET runtime directory, the PowerShell
    install) are always available and are not recorded; in-memory assemblies
    (``Add-Type`` without -OutputAssembly) have no location and cannot be.
    """
    loc = str(origin.get("loc") or "")
    if not loc or bool(origin.get("gac")):
        return False
    return not (_under(loc, str(origin.get("rt") or ""))
                or _under(loc, str(origin.get("pshome") or "")))


def derive_expression(raw: str, origin: MutableMapping[str, Any]) -> str:
    """Creation expression to embed in the stub.

    A ``$variable`` cannot recreate anything in another session, so when the
    runtime type has a public parameterless constructor ``[Type]::new()`` is
    embedded instead. Anything else is kept verbatim.
    """
    cleaned = (raw or "").strip()
    if not cleaned.startswith("$"):
        return cleaned
    type_name = str(origin.get("tn") or "")
    if bool(origin.get("ctor0")) and _PLAIN_TYPE_RE.fullmatch(type_name):
        return f"[{type_name}]::new()"
    return cleaned


def safe_class_name(type_name: str) -> str:
    parts = [segment for segment in type_name.split(".") if segment]
    candidate = parts[-1] if parts else "PSObject"
    cleaned = sanitize_identifier(candidate)
    return cleaned or "PSObject"


def generate(shell, obj: str, output_path: Path, *,
             expression: Optional[str] = None) -> None:
    """Write a Protocol stub for the object `obj` evaluates to.

    `expression` overrides the creation expression embedded as
    ``__ps_expression__`` (needed when `obj` is a ``$variable`` whose type has
    no parameterless constructor).
    """
    shell_needs_stop = False
    if (not shell.is_running):
        shell.start()
        shell_needs_stop = True
    # $PSStyle only exists in PowerShell 7+; Windows PowerShell 5.1 never emits ANSI.
    if getattr(shell, "edition", "core") == "core":
        shell.run("$PSStyle.OutputRendering = 'PlainText'", raise_on_error=False)
    shell.run("$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()", raise_on_error=False)

    from .ps_proxy import build_creation_strategies, static_type_literal

    # A proxy (or a proxy's variable) knows how it was reached; that beats a
    # temporary variable name that means nothing in another session.
    ps_ref = getattr(obj, "ps_ref", None)
    if isinstance(ps_ref, str):
        origin = getattr(obj, "ps_origin", None)
        if expression is None and origin and origin != ps_ref:
            expression = str(origin)
        obj = ps_ref
    elif expression is None:
        known = getattr(shell, "_proxy_origins", None) or {}
        origin = known.get(str(obj).strip())
        if origin and origin != str(obj).strip():
            expression = str(origin)

    strategies = build_creation_strategies(obj)
    errors: List[str] = []
    chosen: Optional[Tuple[str, str, str, List[MutableMapping[str, Any]]]] = None

    try:
        static_inner = static_type_literal(obj)
        if static_inner is not None:
            try:
                type_name, members = fetch_static_members(shell, static_inner)
            except Exception as ex:
                raise RuntimeError(
                    f"Unable to materialise a static type from '{obj}': {ex}") from ex
            origin = fetch_origin(shell, static=True)
            assembly, assembly_name = _assembly_meta(origin)
            static_expression = expression or f"[{static_inner}]"
            # Name the class after the caller's spelling: the reflected
            # FullName of a closed generic is assembly-qualified soup.
            protocol_name = safe_class_name(static_inner)
            source = render_protocol(protocol_name, members,
                                     ps_type_name=type_name,
                                     ps_expression=static_expression, ps_static=True,
                                     ps_assembly=assembly, ps_assembly_name=assembly_name)
            output_path.write_text(source, encoding="utf-8")
            print(f"Generated {output_path} for static {type_name} "
                  f"(expression: {static_expression}"
                  + (f"; assembly: {assembly}" if assembly else "") + ")")
            return

        for label, candidate in strategies:
            try:
                type_name, members = fetch_members(shell, candidate)
                chosen = (label, candidate, type_name, members)
                break
            except Exception as ex:
                errors.append(f"{label}: {ex}")
        if not chosen:
            details = "\n  ".join(errors) or "no strategies produced an object"
            raise RuntimeError(
                f"Unable to materialise an object from '{obj}'. Tried:\n  {details}")

        label, candidate, type_name, members = chosen
        origin = fetch_origin(shell)
        assembly, assembly_name = _assembly_meta(origin)
        final_expression = expression or derive_expression(candidate, origin)
        protocol_name = safe_class_name(type_name)
        source = render_protocol(protocol_name, members,
                                 ps_type_name=type_name, ps_expression=final_expression,
                                 ps_assembly=assembly, ps_assembly_name=assembly_name)
        output_path.write_text(source, encoding="utf-8")
        print(f"Generated {output_path} for {type_name} (strategy: {label}; "
              f"expression: {final_expression}"
              + (f"; assembly: {assembly}" if assembly else "") + ")")
        if final_expression.startswith("$") and not expression:
            print(f"  note: {final_expression} is a variable reference, so "
                  f"make_proxy({protocol_name}) can only bind an existing object; "
                  "pass expression=... to embed a creation expression.")
    finally:
        if shell_needs_stop:
            shell.stop()


def _assembly_meta(origin: MutableMapping[str, Any]) -> Tuple[str, str]:
    if not is_external_assembly(origin):
        return "", ""
    return str(origin.get("loc") or ""), str(origin.get("name") or "")
