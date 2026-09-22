"""Tests for generate_psobject: type mapping, signature parsing and
Protocol generation (unit), plus end-to-end generation against pwsh."""
from __future__ import annotations

import pytest

from virtualshell.generate_psobject import (
    _property_type,
    build_method_signatures,
    categorize_members,
    derive_expression,
    first_signature,
    is_external_assembly,
    map_ps_type,
    parse_parameters,
    property_is_writable,
    render_protocol,
    safe_class_name,
    sanitize_identifier,
    split_signatures,
)


class TestMapPsType:
    def test_scalars(self):
        assert map_ps_type("System.String")[0] == "str"
        assert map_ps_type("System.Int64")[0] == "int"
        assert map_ps_type("bool")[0] == "bool"
        assert map_ps_type("System.Byte")[0] == "int"

    def test_arrays(self):
        # byte[] moves through the bridge as bytes; other arrays are proxies
        # implementing the read-only sequence protocol.
        assert map_ps_type("byte[]")[0] == "bytes"
        assert map_ps_type("System.Byte[]")[0] == "bytes"
        annotation, typing_bits, _ = map_ps_type("int[]")
        assert annotation == "Sequence[int]"
        assert "Sequence" in typing_bits

    def test_char_array_is_str(self):
        # PowerShell's binder converts a string to char[], and List[str]
        # would wrongly accept lists of multi-character strings.
        assert map_ps_type("char[]")[0] == "str"
        assert map_ps_type("System.Char[]")[0] == "str"

    def test_nullable(self):
        annotation, typing_bits, _ = map_ps_type("System.Nullable`1[System.Int32]")
        assert annotation == "Optional[int]"
        assert "Optional" in typing_bits

    def test_generic_dictionary(self):
        annotation, _, _ = map_ps_type(
            "System.Collections.Generic.Dictionary`2[System.String,System.Int32]")
        assert annotation == "Mapping[str, int]"

    def test_generic_collections_are_read_only_protocols(self):
        assert map_ps_type(
            "System.Collections.Generic.List`1[System.String]")[0] == "Sequence[str]"
        assert map_ps_type(
            "System.Collections.ObjectModel.ReadOnlyCollection`1[System.String]")[0] == \
            "Sequence[str]"
        assert map_ps_type(
            "System.Collections.Generic.HashSet`1[System.Int32]")[0] == "AbstractSet[int]"

    def test_unknown_is_any(self):
        assert map_ps_type("Some.Unknown.Type")[0] == "Any"

    def test_datetime_requires_runtime_import(self):
        annotation, _, runtime_bits = map_ps_type("System.DateTime")
        assert annotation == "datetime.datetime"
        assert "datetime" in runtime_bits


class TestParseParameters:
    def test_simple(self):
        params = parse_parameters("string name, int count")
        assert [(p[0], p[1]) for p in params] == [("name", "str"), ("count", "int")]

    def test_array_parameter_annotations(self):
        params = parse_parameters("byte[] buffer, int offset, int count")
        assert params[0][0] == "buffer"
        assert params[0][1] == "bytes"
        assert parse_parameters("string[] names")[0][1] == "Sequence[str]"

    def test_generic_parameter_with_comma_inside(self):
        params = parse_parameters(
            "System.Collections.Generic.Dictionary`2[System.String,System.Int32] map, int other")
        assert len(params) == 2
        assert params[0][1] == "Mapping[str, int]"
        assert params[1] == ("other", "int", params[1][2], params[1][3])

    def test_nameless_parameter_gets_generated_name(self):
        params = parse_parameters("string")
        assert params[0][0] == "arg0"

    def test_keyword_name_sanitized(self):
        params = parse_parameters("string class")
        assert params[0][0] == "class_"

    def test_self_renamed(self):
        params = parse_parameters("object self")
        assert params[0][0] == "self_"


class TestSignatures:
    def test_first_signature_splits_overloads(self):
        assert first_signature("int M(int a), int M(string b)") == "int M(int a)"

    def test_split_signatures(self):
        sigs = split_signatures(
            "int M(int a), string M(System.Collections.Generic.Dictionary`2[string,int] map)")
        assert sigs == [
            "int M(int a)",
            "string M(System.Collections.Generic.Dictionary`2[string,int] map)",
        ]

    def test_static_prefix_stripped(self):
        typing_bits, runtime_bits = set(), set()
        lines = build_method_signatures(
            "Join", {"Definition": "static string Join(string separator, string[] value)"},
            typing_bits, runtime_bits)
        assert len(lines) == 1
        assert "def Join(self, separator: str, value: Sequence[str]) -> str" in lines[0]

    def test_unparseable_falls_back_to_varargs(self):
        typing_bits, runtime_bits = set(), set()
        lines = build_method_signatures("Weird", {"Definition": "?!"},
                                        typing_bits, runtime_bits)
        assert len(lines) == 1
        assert "*args: Any" in lines[0]

    def test_sanitize_identifier(self):
        assert sanitize_identifier("123abc") == "_123abc"
        assert sanitize_identifier("for") == "for_"
        assert sanitize_identifier("a-b c") == "a_b_c"

    def test_safe_class_name(self):
        assert safe_class_name("System.Text.StringBuilder") == "StringBuilder"
        assert safe_class_name("") == "PSObject"


class TestOverloads:
    APPEND = {
        "Name": "Append",
        "MemberType": 64,
        "Definition": (
            "System.Text.StringBuilder Append(char value, int repeatCount), "
            "System.Text.StringBuilder Append(string value), "
            "System.Text.StringBuilder Append(bool value)"
        ),
    }

    def test_every_arity_is_emitted(self):
        typing_bits, runtime_bits = set(), set()
        lines = build_method_signatures("Append", dict(self.APPEND),
                                        typing_bits, runtime_bits)
        text = "\n".join(lines)
        # Single-argument calls must type-check: the 1-arg overload exists.
        assert "def Append(self, value: str) -> Any: ..." in text
        assert "def Append(self, value: str, repeatCount: int) -> Any: ..." in text
        assert text.count("@overload") >= 2
        # A catch-all implementation closes the overload group.
        assert lines[-1] == "    def Append(self, *args: Any, **kwargs: Any) -> Any: ..."
        assert "overload" in typing_bits

    def test_equivalent_overloads_are_deduplicated(self):
        # char and string both map to `str`; bool is separate.
        typing_bits, runtime_bits = set(), set()
        lines = build_method_signatures("Append", dict(self.APPEND),
                                        typing_bits, runtime_bits)
        text = "\n".join(lines)
        assert text.count("def Append(self, value: str) -> Any: ...") == 1
        assert "def Append(self, value: bool) -> Any: ..." in text

    def test_single_overload_has_no_decorator(self):
        typing_bits, runtime_bits = set(), set()
        lines = build_method_signatures(
            "One", {"Definition": "void One(int x)"}, typing_bits, runtime_bits)
        assert lines == ["    def One(self, x: int) -> None: ..."]
        assert "overload" not in typing_bits

    def test_overloaded_protocol_compiles_and_imports_overload(self):
        members = [dict(self.APPEND)]
        source = render_protocol("Sb", members)
        compile(source, "<generated>", "exec")
        assert "overload" in source.split("from typing import ", 1)[1].splitlines()[0]


class TestCategorizeAndWritable:
    def test_categorize(self):
        members = [
            {"Name": "M", "MemberType": "Method", "Definition": "void M()"},
            {"Name": "P", "MemberType": "Property", "Definition": "string P {get;set;}"},
            {"Name": "N", "MemberType": "NoteProperty", "Definition": "string N=x"},
        ]
        grouped = categorize_members(members)
        assert list(grouped["Methods"]) == ["M"]
        assert set(grouped["Properties"]) == {"P", "N"}

    def test_parameterized_property_is_a_method(self):
        # Indexers (StringBuilder.Chars) take arguments and are invoked with
        # method syntax; rendering them as plain properties would produce
        # stubs that cannot be called correctly.
        for member_type in (512, "ParameterizedProperty"):
            members = [{"Name": "Chars", "MemberType": member_type,
                        "Definition": "char Chars(int index) {get;set;}"}]
            grouped = categorize_members(members)
            assert list(grouped["Methods"]) == ["Chars"]
            assert not grouped["Properties"]

    def test_parameterized_property_renders_with_index_argument(self):
        members = [{"Name": "Chars", "MemberType": 512,
                    "Definition": "char Chars(int index) {get;set;}"}]
        source = render_protocol("Sb", members)
        compile(source, "<generated>", "exec")
        assert "def Chars(self, index: int) -> str: ..." in source
        assert "@property\n    def Chars" not in source

    def test_property_writable_from_definition(self):
        assert property_is_writable({"Definition": "string P {get;set;}"}) is True
        assert property_is_writable({"Definition": "string P {get;}"}) is False

    def test_note_property_is_writable(self):
        assert property_is_writable(
            {"MemberType": "NoteProperty", "Definition": "string N=x"}) is True


class TestRenderProtocol:
    MEMBERS = [
        {"Name": "Length", "MemberType": "Property",
         "TypeNameOfValue": "System.Int32", "Definition": "int Length {get;}"},
        {"Name": "Name", "MemberType": "Property",
         "TypeNameOfValue": "System.String", "Definition": "string Name {get;set;}"},
        {"Name": "Read", "MemberType": "Method",
         "Definition": "int Read(byte[] buffer, int offset, int count)"},
    ]

    def test_generated_source_is_valid_python(self):
        source = render_protocol("MyProxy", self.MEMBERS)
        compile(source, "<generated>", "exec")   # must not raise

    def test_contains_expected_stubs(self):
        source = render_protocol("MyProxy", self.MEMBERS)
        assert "class MyProxy(Protocol):" in source
        assert "def Length(self) -> int: ..." in source
        assert "def Name(self, value: str) -> None: ..." in source   # setter
        assert "def Read(self, buffer: bytes, offset: int, count: int) -> int: ..." in source
        assert "def proxy_schema(self) -> Dict[str, Any]: ..." in source
        assert "list[Any]" not in source   # py3.8-compatible typing only

    def test_readonly_property_has_no_setter(self):
        source = render_protocol("MyProxy", self.MEMBERS)
        assert "def Length(self, value" not in source

    def test_metadata_classvars_embedded_when_provided(self):
        source = render_protocol(
            "MyProxy", self.MEMBERS,
            ps_type_name="My.Type", ps_expression="[My.Type]::new()")
        compile(source, "<generated>", "exec")
        assert "__ps_type_name__: ClassVar[str] = 'My.Type'" in source
        assert "__ps_expression__: ClassVar[str] = '[My.Type]::new()'" in source
        assert "ClassVar" in source.split("from typing import ", 1)[1].splitlines()[0]

    def test_no_metadata_classvars_by_default(self):
        source = render_protocol("MyProxy", self.MEMBERS)
        assert "__ps_type_name__" not in source
        assert "__ps_static__" not in source


class TestStaticRendering:
    # Shaped like `[System.Math] | Get-Member -Static | ConvertTo-Json`.
    MEMBERS = [
        {"Name": "PI", "MemberType": 4,
         "Definition": "static double PI {get;}"},
        {"Name": "Sqrt", "MemberType": 64,
         "Definition": "static double Sqrt(double d)"},
    ]

    def test_static_property_type_strips_prefix(self):
        assert _property_type(
            {"Definition": "static datetime Now {get;}"}) == "datetime"
        # Instance definitions are unaffected.
        assert _property_type({"Definition": "int Year {get;}"}) == "int"

    def test_ps_static_classvar_embedded(self):
        source = render_protocol(
            "Math", self.MEMBERS,
            ps_type_name="System.Math", ps_expression="[System.Math]",
            ps_static=True)
        compile(source, "<generated>", "exec")
        assert "__ps_static__: ClassVar[bool] = True" in source
        assert "__ps_expression__: ClassVar[str] = '[System.Math]'" in source

    def test_static_members_render_with_real_types(self):
        source = render_protocol("Math", self.MEMBERS, ps_static=True)
        compile(source, "<generated>", "exec")
        assert "def PI(self) -> float: ..." in source
        assert "def Sqrt(self, d: float) -> float: ..." in source


# =============================================================================
# Integration
# =============================================================================

from conftest import integration  # noqa: E402  (pwsh and/or Windows PowerShell 5.1)


@pytest.fixture(scope="module")
def shell(edition):
    from virtualshell import Shell

    sh = Shell(timeout=60, powershell_edition=edition).start()
    yield sh
    sh.stop(force=True)


@integration
class TestGenerateEndToEnd:

    def test_generate_for_datetime(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "datetime_proxy.py"
        generate(shell, "Get-Date", out_file)
        source = out_file.read_text(encoding="utf-8")

        compile(source, str(out_file), "exec")   # generated file must parse
        assert "class DateTime(Protocol):" in source
        assert "def Year(self) -> int: ..." in source
        assert "def AddDays(self" in source
        # Statics of the type are listed too (Get-Member -Static), tagged.
        assert "def IsLeapYear(self, year: int) -> bool: ...  # static" in source
        assert "def ReferenceEquals" not in source

    def test_generate_for_custom_object(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "custom.py"
        generate(shell, "[pscustomobject]@{ Navn = 'x'; Tall = 42 }", out_file)
        source = out_file.read_text(encoding="utf-8")
        compile(source, str(out_file), "exec")
        assert "def Navn(self) -> str: ..." in source
        assert "def Tall(self) -> int: ..." in source

    def test_generate_failure_raises(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        with pytest.raises(RuntimeError, match="Unable to materialise"):
            generate(shell, "Completely-Bogus-Cmdlet-Xyz", tmp_path / "nope.py")

    def test_generated_protocol_matches_live_proxy(self, shell, tmp_path):
        """The generated stub names must exist on the live proxy."""
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "sb.py"
        generate(shell, "System.Text.StringBuilder()", out_file)
        source = out_file.read_text(encoding="utf-8")
        compile(source, str(out_file), "exec")

        proxy = shell.make_proxy("", "System.Text.StringBuilder")
        live = set(dir(proxy))
        for expected in ("Append", "ToString", "Length"):
            assert expected in source
            assert expected in live

        # Overloaded methods must expose every arity so single-argument
        # calls type-check (Append has both 1- and 2-argument overloads).
        assert "@overload" in source
        assert "def Append(self, value: str) -> " in source

        # The Chars indexer must be a callable method in BOTH the stub and
        # the live proxy - never an argument-less property.
        assert "def Chars(self, index: int) -> str: ..." in source
        assert "@property\n    def Chars" not in source
        assert "Chars" in live

    def test_make_proxy_from_generated_protocol_class(self, shell, tmp_path):
        """generate -> import -> make_proxy(ProtocolClass) round trip."""
        import importlib.util

        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "sb_protocol.py"
        generate(shell, "System.Text.StringBuilder()", out_file)

        spec = importlib.util.spec_from_file_location("sb_protocol", out_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        StringBuilder = module.StringBuilder

        assert StringBuilder.__ps_type_name__ == "System.Text.StringBuilder"

        # Create a fresh object from the embedded expression.
        sb = shell.make_proxy(StringBuilder)
        sb.Append("via protokoll-klassen")
        assert sb.ToString() == "via protokoll-klassen"
        assert sb.type_name == "System.Text.StringBuilder"

        # Bind an existing variable while keeping the protocol typing.
        shell.run("$vs_gen_bind = [System.Text.StringBuilder]::new()",
                  raise_on_error=True)
        bound = shell.make_proxy(StringBuilder, "$vs_gen_bind")
        bound.Append("bundet")
        assert shell.run("$vs_gen_bind.ToString()").out.strip() == "bundet"

    def test_generate_static_type(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "math_static.py"
        generate(shell, "[System.Math]", out_file)
        source = out_file.read_text(encoding="utf-8")

        compile(source, str(out_file), "exec")
        assert "class Math(Protocol):" in source
        assert "__ps_static__: ClassVar[bool] = True" in source
        assert "__ps_expression__: ClassVar[str] = '[System.Math]'" in source
        assert "def Sqrt(self" in source
        assert "def PI(self) -> float: ..." in source

    def test_make_proxy_from_generated_static_protocol(self, shell, tmp_path):
        """generate -> import -> make_proxy(StaticProtocol) round trip."""
        import importlib.util

        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "math_protocol.py"
        generate(shell, "[System.Math]", out_file)

        spec = importlib.util.spec_from_file_location("math_protocol", out_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        Math = module.Math

        assert Math.__ps_static__ is True

        m = shell.make_proxy(Math)
        assert m.Sqrt(16.0) == 4.0
        assert abs(m.PI - 3.141592653589793) < 1e-12


class TestOriginMetadata:
    RT = "C:\\Windows\\Microsoft.NET\\Framework64\\v4.0.30319\\"
    PSHOME = "C:\\Program Files\\PowerShell\\7"

    def _origin(self, **over):
        base = {"loc": "C:\\Vendor\\Api\\Vendor.Api.dll", "name": "Vendor.Api, Version=1.0",
                "gac": False, "rt": self.RT, "pshome": self.PSHOME,
                "tn": "Vendor.Api.Client", "ctor0": True}
        base.update(over)
        return base

    def test_vendor_assembly_is_external(self):
        assert is_external_assembly(self._origin())

    def test_gac_runtime_and_pshome_are_not_external(self):
        assert not is_external_assembly(self._origin(gac=True))
        assert not is_external_assembly(self._origin(loc=self.RT + "mscorlib.dll"))
        assert not is_external_assembly(
            self._origin(loc=self.PSHOME + "\\System.Management.Automation.dll"))
        assert not is_external_assembly(self._origin(loc=""))          # in-memory Add-Type
        assert not is_external_assembly({})

    def test_forward_slashes_and_case_are_normalized(self):
        assert not is_external_assembly(
            self._origin(loc="c:/program files/powershell/7/pwsh.dll"))

    def test_variable_with_default_ctor_becomes_new_expression(self):
        assert derive_expression("$sb", self._origin()) == "[Vendor.Api.Client]::new()"

    def test_variable_without_default_ctor_stays_a_variable(self):
        assert derive_expression("$tia", self._origin(ctor0=False)) == "$tia"

    def test_generic_type_is_not_bracketed(self):
        origin = self._origin(tn="System.Collections.Generic.List`1[[System.String]]")
        assert derive_expression("$l", origin) == "$l"

    def test_non_variable_expressions_are_kept(self):
        assert derive_expression("[Vendor.Api.Client]::new(3)", self._origin()) == \
            "[Vendor.Api.Client]::new(3)"

    def test_render_protocol_embeds_assembly_metadata(self):
        source = render_protocol("Client", [], ps_type_name="Vendor.Api.Client",
                                 ps_expression="[Vendor.Api.Client]::new()",
                                 ps_assembly="C:\\Vendor\\Api\\Vendor.Api.dll",
                                 ps_assembly_name="Vendor.Api, Version=1.0")
        compile(source, "<generated>", "exec")
        assert r"__ps_assembly__: ClassVar[str] = 'C:\\Vendor\\Api\\Vendor.Api.dll'" in source
        assert "__ps_assembly_name__: ClassVar[str] = 'Vendor.Api, Version=1.0'" in source

    def test_render_protocol_without_assembly_has_no_assembly_lines(self):
        source = render_protocol("Sb", [], ps_type_name="System.Text.StringBuilder",
                                 ps_expression="[System.Text.StringBuilder]::new()")
        assert "__ps_assembly__" not in source


def _load_module(path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@integration
class TestGenerateReconstruction:
    """The stub alone must be enough for make_proxy() in a brand-new session."""

    def test_framework_type_records_no_assembly(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "sb.py"
        shell.run("$vs_gen_sb = [System.Text.StringBuilder]::new()", raise_on_error=True)
        generate(shell, "$vs_gen_sb", out_file)
        source = out_file.read_text(encoding="utf-8")
        assert "__ps_assembly__" not in source
        # A $variable with a parameterless ctor becomes a creation expression.
        assert "__ps_expression__: ClassVar[str] = '[System.Text.StringBuilder]::new()'" in source

    def test_explicit_expression_overrides(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "sb2.py"
        shell.run("$vs_gen_sb2 = [System.Text.StringBuilder]::new(8)", raise_on_error=True)
        generate(shell, "$vs_gen_sb2", out_file,
                 expression="[System.Text.StringBuilder]::new(64)")
        proto = _load_module(out_file).StringBuilder
        assert proto.__ps_expression__ == "[System.Text.StringBuilder]::new(64)"
        assert shell.make_proxy(proto).Capacity == 64
        assert shell.make_proxy(proto, "").Capacity == 64     # "" == use the stub's expression

    def test_external_assembly_round_trips_into_fresh_session(self, shell, edition, tmp_path):
        from virtualshell import Shell
        from virtualshell.generate_psobject import generate

        dll = tmp_path / "VsTestWidget.dll"
        code = ("namespace VsTest { public class Widget { public int Value = 7; "
                "public string Hello(string n) { return \"hi \" + n; } } }")
        res = shell.run(
            f"Add-Type -TypeDefinition '{code}' -OutputAssembly '{dll}'", timeout=120)
        if not res.success or not dll.exists():
            pytest.skip(f"Add-Type -OutputAssembly unavailable here: {res.err[:200]}")
        shell.run(f"[Reflection.Assembly]::LoadFrom('{dll}') | Out-Null; "
                  "$vs_widget = [VsTest.Widget]::new()", raise_on_error=True)

        out_file = tmp_path / "Widget.py"
        generate(shell, "$vs_widget", out_file)
        proto = _load_module(out_file).Widget
        assert proto.__ps_expression__ == "[VsTest.Widget]::new()"
        assert proto.__ps_assembly__.lower().replace("/", "\\") == str(dll).lower()
        # pwsh names the emitted assembly after a temp file, 5.1 after the dll;
        # either way it is the manifest name LoadFrom reports again later.
        assert "Version=" in proto.__ps_assembly_name__

        # Brand-new host: nothing loaded, no $vs_widget. The stub must suffice.
        with Shell(timeout=60, powershell_edition=edition) as fresh:
            widget = fresh.make_proxy(proto)
            assert widget.Hello("x") == "hi x"
            assert widget.Value == 7
            assert fresh.run("[VirtualShell.AssemblyDirResolver]::Directories()").out.strip()
            # Second proxy: assembly already loaded, no double load.
            assert fresh.make_proxy(proto).Value == 7
