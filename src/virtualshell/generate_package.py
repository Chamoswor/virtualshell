"""Package generation for ``generate_psobject(follow=True)``.

One Protocol module per .NET type reachable from a root type, cross-annotated
so that ``tia.Projects`` is typed ``ProjectComposition`` and
``projects.Item(0)`` is typed ``Project``: completion all the way down the
object graph. The graph is walked by reflection in the PowerShell session
(``type_graph.ps1``), so no live instance of the non-root types is needed.

Type mapping: generated protocols for followed (SDK) types, ``str`` for enums
(proxies surface them as strings), the scalar table of `generate_psobject`
for runtime types, ``Any`` otherwise. Generic collections map to
``List[...]`` / ``Dict[...]`` like the single-file generator does.
"""
from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Set, Tuple

from .generate_psobject import (
    derive_expression,
    fetch_origin,
    map_ps_type,
    safe_class_name,
    sanitize_identifier,
)

_TYPE_GRAPH_HELPER = Path(__file__).resolve().parent / "type_graph.ps1"

# .NET collections come back as proxies that implement len(), indexing,
# iteration and `in` (see PsProxy), i.e. read-only Python protocols: so
# Sequence / Mapping / AbstractSet, never List / Dict / Set, which would
# promise append(), item assignment and the like.
_GENERIC_SEQUENCE = {
    "System.Collections.Generic.List`1",
    "System.Collections.Generic.IList`1",
    "System.Collections.Generic.IEnumerable`1",
    "System.Collections.Generic.ICollection`1",
    "System.Collections.Generic.IReadOnlyList`1",
    "System.Collections.Generic.IReadOnlyCollection`1",
    "System.Collections.ObjectModel.ReadOnlyCollection`1",
    "System.Collections.ObjectModel.Collection`1",
    "System.Collections.ObjectModel.ObservableCollection`1",
    "System.Collections.Generic.Queue`1",
    "System.Collections.Generic.Stack`1",
    "System.Collections.Generic.LinkedList`1",
}
_GENERIC_MAPPING = {
    "System.Collections.Generic.Dictionary`2",
    "System.Collections.Generic.IDictionary`2",
    "System.Collections.Generic.IReadOnlyDictionary`2",
    "System.Collections.Generic.SortedDictionary`2",
    "System.Collections.Concurrent.ConcurrentDictionary`2",
}
_GENERIC_SET = {
    "System.Collections.Generic.HashSet`1",
    "System.Collections.Generic.ISet`1",
    "System.Collections.Generic.SortedSet`1",
    "System.Collections.Generic.IReadOnlySet`1",
}
_NULLABLE = "System.Nullable`1"
_BYTE_ARRAYS = {"System.Byte[]", "byte[]", "Byte[]"}
_CHAR_ARRAYS = {"System.Char[]", "char[]", "Char[]"}


def split_generic(type_str: str) -> Optional[Tuple[str, List[str]]]:
    """'Def`1[A,B`1[C]]' -> ('Def`1', ['A', 'B`1[C]']); None when not generic."""
    if "`" not in type_str or not type_str.endswith("]"):
        return None
    start = type_str.find("[")
    if start == -1:
        return None
    definition = type_str[:start]
    args: List[str] = []
    depth = 0
    current = ""
    for ch in type_str[start + 1:-1]:
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        if ch == "," and depth == 0:
            args.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        args.append(current.strip())
    return definition, args


class Annotator:
    """Maps reflected .NET type names to annotations, preferring generated protocols."""

    def __init__(self, names: Dict[str, str], enums: Set[str]):
        self.names = names          # full .NET name -> generated class name
        self.enums = enums          # full .NET names of enum types
        self.typing_bits: Set[str] = set()
        self.runtime_bits: Set[str] = set()
        self.imports: Set[str] = set()

    def __call__(self, type_str: Any) -> str:
        s = str(type_str or "").strip()
        if s in self.names:
            self.imports.add(self.names[s])
            return self.names[s]
        if s in self.enums:
            return "str"
        if s in _BYTE_ARRAYS:
            return "bytes"           # byte[] travels through the bridge as bytes
        if s in _CHAR_ARRAYS:
            return "str"             # PowerShell binds a string to char[]
        if s.endswith("[]"):
            self.typing_bits.add("Sequence")
            return f"Sequence[{self(s[:-2])}]"
        generic = split_generic(s)
        if generic:
            definition, args = generic
            if definition in _GENERIC_SEQUENCE and len(args) == 1:
                self.typing_bits.add("Sequence")
                return f"Sequence[{self(args[0])}]"
            if definition in _GENERIC_MAPPING and len(args) == 2:
                self.typing_bits.add("Mapping")
                return f"Mapping[{self(args[0])}, {self(args[1])}]"
            if definition in _GENERIC_SET and len(args) == 1:
                self.typing_bits.add("AbstractSet")
                return f"AbstractSet[{self(args[0])}]"
            if definition == _NULLABLE and len(args) == 1:
                self.typing_bits.add("Optional")
                return f"Optional[{self(args[0])}]"
        annotation, t_bits, r_bits = map_ps_type(s)
        self.typing_bits.update(t_bits)
        self.runtime_bits.update(r_bits)
        return annotation


def allocate_class_names(full_names: Iterable[str]) -> Dict[str, str]:
    """Short class/module names per .NET full name, disambiguated on collision.

    'A.B.Device' -> 'Device'; when 'A.C.Device' also exists they become
    'B_Device' and 'C_Device'.
    """
    ordered = list(OrderedDict.fromkeys(full_names))
    by_short: Dict[str, List[str]] = {}
    for full in ordered:
        by_short.setdefault(safe_class_name(full), []).append(full)
    names: Dict[str, str] = {}
    taken: Set[str] = set()
    for short, fulls in by_short.items():
        if len(fulls) == 1:
            names[fulls[0]] = short
            taken.add(short)
            continue
        for full in fulls:
            parts = [p for p in full.replace("+", ".").split(".") if p]
            base = sanitize_identifier("_".join(parts[-2:])) if len(parts) > 1 else short
            candidate = base
            n = 2
            while candidate in taken:
                candidate = f"{base}{n}"
                n += 1
            names[full] = candidate
            taken.add(candidate)
    return names


def _sanitize_param(name: Any, index: int) -> str:
    safe = sanitize_identifier(str(name or f"arg{index}"))
    return "self_" if safe == "self" else safe


STATIC_TAG = "  # static"


def _tag(line: str, entry: MutableMapping[str, Any]) -> str:
    """Mark members that are static in .NET; proxies call them via [Type]::Name."""
    return line + STATIC_TAG if entry.get("s") else line


def _render_members(desc: MutableMapping[str, Any],
                    annotate: Annotator) -> Tuple[List[str], List[str]]:
    prop_lines: List[str] = []
    method_lines: List[str] = []

    for prop in desc.get("props") or []:
        name = str(prop.get("n") or "")
        if not name:
            continue
        safe = sanitize_identifier(name)
        annotation = annotate(prop.get("t"))
        index_params = prop.get("ip") or []
        if index_params:
            # Indexer (this[...]): callable, like a ParameterizedProperty.
            params = ", ".join(f"{_sanitize_param(p.get('n'), i)}: {annotate(p.get('t'))}"
                               for i, p in enumerate(index_params))
            method_lines.append(_tag(f"    def {safe}(self, {params}) -> {annotation}: ...", prop))
            continue
        prop_lines.append("    @property")
        prop_lines.append(_tag(f"    def {safe}(self) -> {annotation}: ...", prop))
        if prop.get("w"):
            prop_lines.append(f"    @{safe}.setter")
            prop_lines.append(_tag(f"    def {safe}(self, value: {annotation}) -> None: ...", prop))
        prop_lines.append("")

    def signature(params: List[Tuple[str, str]]) -> str:
        return "".join(f", {n}: {a}" for n, a in params)

    seen_names: Set[str] = set()
    for meth in desc.get("meths") or []:
        name = str(meth.get("n") or "")
        if not name:
            continue
        safe = sanitize_identifier(name)
        if safe in seen_names:
            continue
        seen_names.add(safe)
        signatures: List[Tuple[List[Tuple[str, str]], str]] = []
        seen_sigs: Set[Tuple[str, ...]] = set()
        for overload_ in meth.get("o") or []:
            params = [(_sanitize_param(p.get("n"), i), annotate(p.get("t")))
                      for i, p in enumerate(overload_.get("p") or [])]
            ret = annotate(overload_.get("r") or "System.Void")
            key = tuple(a for _, a in params)
            if key in seen_sigs:
                continue
            seen_sigs.add(key)
            signatures.append((params, ret))

        if len(signatures) == 1:
            params, ret = signatures[0]
            method_lines.append(_tag(f"    def {safe}(self{signature(params)}) -> {ret}: ...", meth))
        elif signatures:
            annotate.typing_bits.add("overload")
            for params, ret in signatures:
                method_lines.append("    @overload")
                method_lines.append(_tag(f"    def {safe}(self{signature(params)}) -> {ret}: ...", meth))
            method_lines.append(_tag(f"    def {safe}(self, *args: Any, **kwargs: Any) -> Any: ...", meth))
            method_lines.append("")

    if prop_lines and prop_lines[-1] == "":
        prop_lines.pop()
    if method_lines and method_lines[-1] == "":
        method_lines.pop()
    return prop_lines, method_lines


def render_type_module(desc: MutableMapping[str, Any], class_name: str,
                       names: Dict[str, str], enums: Set[str], *,
                       expression: str = "") -> str:
    """Source of one package module: a Protocol for the reflected type `desc`."""
    annotate = Annotator(names, enums)
    prop_lines, method_lines = _render_members(desc, annotate)

    typing_bits = {"Protocol", "ClassVar", "Any", "List", "Dict", "Callable"} | annotate.typing_bits
    imports = sorted(n for n in annotate.imports if n != class_name)
    if imports:
        typing_bits.add("TYPE_CHECKING")

    lines: List[str] = [
        "# This file was generated by virtualshell.generate_psobject (follow=True)",
        "from __future__ import annotations",
        "",
        "from typing import " + ", ".join(sorted(typing_bits, key=str.lower)),
    ]
    for module in sorted(annotate.runtime_bits):
        lines.append(f"import {module}")
    if imports:
        lines.append("")
        lines.append("if TYPE_CHECKING:")
        for name in imports:
            lines.append(f"    from .{name} import {name}")
    lines += ["", f"__all__ = [{class_name!r}]", "", f"class {class_name}(Protocol):"]

    type_name = str(desc.get("tn") or "")
    lines.append(f"    __ps_type_name__: ClassVar[str] = {type_name!r}")
    if expression:
        lines.append(f"    __ps_expression__: ClassVar[str] = {expression!r}")
    assembly = str(desc.get("asm") or "")
    if desc.get("ext") and assembly:
        lines.append(f"    __ps_assembly__: ClassVar[str] = {assembly!r}")
        assembly_name = str(desc.get("asmName") or "")
        if assembly_name:
            lines.append(f"    __ps_assembly_name__: ClassVar[str] = {assembly_name!r}")
    lines.append("")

    if prop_lines:
        lines += prop_lines
        lines.append("")
    if method_lines:
        lines += method_lines
        lines.append("")
    lines += [
        "    def proxy_multi_call(self, func: Callable[..., Any], *args: Any) -> List[Any]: ...",
        "    def proxy_select(self, *expressions: str, **aliases: str) -> List[Dict[str, Any]]: ...",
        "    def generic(self, name: str, *type_args: Any) -> Callable[..., Any]: ...",
        "    def proxy_schema(self) -> Dict[str, Any]: ...",
        "",
        "    @property",
        "    def type_name(self) -> str: ...",
        "",
        "    @property",
        "    def ps_ref(self) -> str: ...",
        "",
        "    @property",
        "    def ps_origin(self) -> str: ...",
        "",
    ]
    return "\n".join(lines)


def _materialize_root(shell, obj: str) -> Tuple[str, str]:
    """Evaluate `obj` into $obj; return (expression used, runtime type full name)."""
    from .ps_proxy import build_creation_strategies

    shell.run("Remove-Variable obj -ErrorAction SilentlyContinue", raise_on_error=False)
    errors: List[str] = []
    for label, candidate in build_creation_strategies(obj):
        res = shell.run(f"$obj = ({candidate})", raise_on_error=False)
        if not res.success:
            errors.append(f"{label}: {(res.err or '').strip()[:200]}")
            continue
        type_name = (shell.run("if ($null -ne $obj) { $obj.GetType().FullName }").out or "").strip()
        if type_name:
            return candidate, type_name
        errors.append(f"{label}: produced null")
    raise RuntimeError(f"Unable to materialise an object from '{obj}'. Tried:\n  "
                       + "\n  ".join(errors))


def parse_type_graph(output: str) -> List[Dict[str, Any]]:
    """Parse the NDJSON emitted by Get-VsTypeGraph."""
    descriptors: List[Dict[str, Any]] = []
    for line in (output or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict) and item.get("tn"):
            descriptors.append(item)
    return descriptors


def generate_package(shell, obj: Any, package_dir: Path, *,
                     expression: Optional[str] = None,
                     include_namespaces: Optional[List[str]] = None,
                     max_types: int = 2000) -> List[Path]:
    """Write a package of cross-annotated Protocol stubs for the type graph of `obj`.

    `obj` is a creation expression, a ``$variable``, a bare ``[Type]`` literal
    (no instance needed) or a `PsProxy`. `include_namespaces` restricts the
    walk to types in those namespaces (nested namespaces included); empty
    means every type outside the .NET runtime. See `Shell.generate_psobject`.
    Returns the written paths (one module per type plus ``__init__.py``).
    """
    from .ps_proxy import _ps_quote, helper_prelude, static_type_literal

    package_dir = Path(package_dir)
    shell_needs_stop = False
    if not shell.is_running:
        shell.start()
        shell_needs_stop = True
    try:
        if getattr(shell, "edition", "core") == "core":
            shell.run("$PSStyle.OutputRendering = 'PlainText'", raise_on_error=False)
        shell.run("$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()",
                  raise_on_error=False)

        # -- root: which type, and how to reach it again ----------------------
        ps_ref = getattr(obj, "ps_ref", None)
        if isinstance(ps_ref, str):
            origin = getattr(obj, "ps_origin", None)
            if expression is None and origin and origin != ps_ref:
                expression = str(origin)
            obj = ps_ref
        obj_text = str(obj).strip()
        if expression is None:
            known = getattr(shell, "_proxy_origins", None) or {}
            origin = known.get(obj_text)
            if origin and origin != obj_text:
                expression = str(origin)

        static_inner = static_type_literal(obj_text)
        if static_inner is not None:
            root_type = static_inner
            root_expression = expression or ""
        else:
            candidate, root_type = _materialize_root(shell, obj_text)
            root_expression = expression or derive_expression(candidate, fetch_origin(shell))

        # -- walk the graph in one round trip ---------------------------------
        lines = helper_prelude(_TYPE_GRAPH_HELPER, "Get-VsTypeGraph")
        cmd = f"Get-VsTypeGraph -RootType {_ps_quote(root_type)} -MaxTypes {int(max_types)}"
        namespaces = [str(ns).strip() for ns in (include_namespaces or []) if str(ns).strip()]
        if namespaces:
            cmd += " -IncludeNamespaces " + ",".join(_ps_quote(ns) for ns in namespaces)
        lines.append(cmd)
        res = shell.run("\n".join(lines), timeout=600.0)
        if not res.success:
            raise RuntimeError(f"Type reflection failed: {(res.err or '').strip()[:500]}")

        descriptors = parse_type_graph(res.out or "")
        enums = {str(d["tn"]) for d in descriptors if d.get("isEnum")}
        # Enum records are emitted as they are discovered, i.e. possibly before
        # the record of the type that referenced them; order the root first.
        types = [d for d in descriptors if not d.get("isEnum")]
        types.sort(key=lambda d: 0 if str(d.get("tn")) == root_type else 1)
        if not types or str(types[0].get("tn")) != root_type:
            raise RuntimeError(
                f"Type reflection returned no data for {root_type}: "
                f"{(res.err or res.out or '').strip()[:400]}")
        names = allocate_class_names(str(d["tn"]) for d in types)

        # -- write the package ------------------------------------------------
        package_dir.mkdir(parents=True, exist_ok=True)
        written: List[Path] = []
        for desc in types:
            class_name = names[str(desc["tn"])]
            is_root = str(desc["tn"]) == root_type
            source = render_type_module(desc, class_name, names, enums,
                                        expression=root_expression if is_root else "")
            path = package_dir / f"{class_name}.py"
            path.write_text(source, encoding="utf-8")
            written.append(path)

        ordered = [names[str(d["tn"])] for d in types]
        init_lines = [
            "# This package was generated by virtualshell.generate_psobject (follow=True)",
            "from __future__ import annotations",
            "",
        ]
        init_lines += [f"from .{n} import {n}" for n in ordered]
        init_lines += ["", "__all__ = [", *[f"    {n!r}," for n in ordered], "]", ""]
        init_path = package_dir / "__init__.py"
        init_path.write_text("\n".join(init_lines), encoding="utf-8")
        written.append(init_path)

        print(f"Generated package {package_dir} with {len(types)} types "
              f"({len(enums)} enums as str); root {names[root_type]} "
              + (f"(expression: {root_expression})" if root_expression
                 else "(no creation expression; bind with obj_ref)"))
        return written
    finally:
        if shell_needs_stop:
            shell.stop()
