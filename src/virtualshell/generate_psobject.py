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


def build_method_signature(name: str, entry: MutableMapping[str, Any], typing_bits: Set[str], runtime_bits: Set[str]) -> str:
    definition: Any = entry.get("Definition")
    if isinstance(definition, list) and definition:
        definition = definition[0]
    if not isinstance(definition, str):
        overloads: Any = entry.get("OverloadDefinitions")
        if isinstance(overloads, list) and overloads:
            definition = overloads[0]
        else:
            definition = ""

    definition = first_signature(definition.strip())
    # Static members are rendered as "static <ret> Name(...)" by Get-Member.
    definition = re.sub(r"^\s*static\s+", "", definition)
    match = METHOD_PATTERN.match(definition)
    if not match:
        typing_bits.add("Any")
        return f"    def {name}(self, *args: Any, **kwargs: Any) -> Any: ..."

    return_ann, extra_typing, extra_runtime = map_ps_type(match.group("ret"))
    typing_bits.update(extra_typing)
    runtime_bits.update(extra_runtime)

    parameters = parse_parameters(match.group("params"))
    params_text = ", ".join(f"{param}: {annotation}" for param, annotation, t_bits, r_bits in parameters)
    for _, _, t_bits, r_bits in parameters:
        typing_bits.update(t_bits)
        runtime_bits.update(r_bits)

    if params_text:
        return f"    def {name}(self, {params_text}) -> {return_ann}: ..."
    return f"    def {name}(self) -> {return_ann}: ..."


PROPERTY_FLAGS = {1, 2, 4, 16, 32, 512}
METHOD_FLAGS = {64, 128, 256}


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
        if "Method" in member_type_text:
            methods.setdefault(name, entry)
        elif "Property" in member_type_text:
            properties.setdefault(name, entry)
        elif member_type_text in {"NoteProperty", "AliasProperty"}:
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

    return False


def render_protocol(class_name: str, members: Iterable[MutableMapping[str, Any]]) -> str:
    grouped = categorize_members(members)
    typing_bits: Set[str] = {"Protocol"}
    runtime_bits: Set[str] = set()

    prop_lines: List[str] = []
    for original_name, entry in grouped["Properties"].items():
        safe_name = sanitize_identifier(original_name)
        annotation, t_bits, r_bits = map_ps_type(str(entry.get("TypeNameOfValue", "")))
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
        line = build_method_signature(safe_name, entry, typing_bits, runtime_bits)
        method_lines.append(line)

    method_lines.append("    def proxy_multi_call(self, func: Callable[..., Any], *args: Any) -> List[Any]: ...")
    method_lines.append("    def proxy_schema(self) -> Dict[str, Any]: ...")
    method_lines.append("")
    method_lines.append("    @property")
    method_lines.append("    def type_name(self) -> str: ...")
    method_lines.append("")
    method_lines.append("    @property")
    method_lines.append("    def ps_ref(self) -> str: ...")
    typing_bits.update({"Any", "List", "Dict", "Callable"})

    if prop_lines and prop_lines[-1] == "":
        prop_lines.pop()

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

    if not prop_lines and not method_lines:
        lines.append("    ...")
    else:
        lines.extend(prop_lines)
        if prop_lines and method_lines:
            lines.append("")
        lines.extend(method_lines)

    lines.append("")
    return "\n".join(lines)


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
    raw_text = (raw_result.out or "").strip()
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
    return type_name, members


def safe_class_name(type_name: str) -> str:
    parts = [segment for segment in type_name.split(".") if segment]
    candidate = parts[-1] if parts else "PSObject"
    cleaned = sanitize_identifier(candidate)
    return cleaned or "PSObject"


def generate(shell, obj: str, output_path: Path) -> None:
    shell_needs_stop = False
    if (not shell.is_running):
        shell.start()
        shell_needs_stop = True
    shell.run("$PSStyle.OutputRendering = 'PlainText'", raise_on_error=False)
    shell.run("$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()", raise_on_error=False)

    from .ps_proxy import build_creation_strategies

    strategies = build_creation_strategies(obj)
    errors: List[str] = []
    chosen: Optional[Tuple[str, str, str, List[MutableMapping[str, Any]]]] = None

    try:
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

        label, expression, type_name, members = chosen
        protocol_name = safe_class_name(type_name)
        source = render_protocol(protocol_name, members)
        output_path.write_text(source, encoding="utf-8")
        print(f"Generated {output_path} for {type_name} (strategy: {label}; expression: {expression})")
    finally:
        if shell_needs_stop:
            shell.stop()
