"""Tests for generate_psobject: type mapping, signature parsing and
Protocol generation (unit), plus end-to-end generation against pwsh."""
from __future__ import annotations

import shutil

import pytest

from virtualshell.generate_psobject import (
    build_method_signature,
    categorize_members,
    first_signature,
    map_ps_type,
    parse_parameters,
    property_is_writable,
    render_protocol,
    safe_class_name,
    sanitize_identifier,
)


class TestMapPsType:
    def test_scalars(self):
        assert map_ps_type("System.String")[0] == "str"
        assert map_ps_type("System.Int64")[0] == "int"
        assert map_ps_type("bool")[0] == "bool"
        assert map_ps_type("System.Byte")[0] == "int"

    def test_array_becomes_list(self):
        annotation, typing_bits, _ = map_ps_type("byte[]")
        assert annotation == "List[int]"
        assert "List" in typing_bits

    def test_nullable(self):
        annotation, typing_bits, _ = map_ps_type("System.Nullable`1[System.Int32]")
        assert annotation == "Optional[int]"
        assert "Optional" in typing_bits

    def test_generic_dictionary(self):
        annotation, _, _ = map_ps_type(
            "System.Collections.Generic.Dictionary`2[System.String,System.Int32]")
        assert annotation == "Dict[str, int]"

    def test_generic_list(self):
        assert map_ps_type(
            "System.Collections.Generic.List`1[System.String]")[0] == "List[str]"

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

    def test_array_parameter_keeps_list_annotation(self):
        params = parse_parameters("byte[] buffer, int offset, int count")
        assert params[0][0] == "buffer"
        assert params[0][1] == "List[int]"

    def test_generic_parameter_with_comma_inside(self):
        params = parse_parameters(
            "System.Collections.Generic.Dictionary`2[System.String,System.Int32] map, int other")
        assert len(params) == 2
        assert params[0][1] == "Dict[str, int]"
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

    def test_static_prefix_stripped(self):
        typing_bits, runtime_bits = set(), set()
        line = build_method_signature(
            "Join", {"Definition": "static string Join(string separator, string[] value)"},
            typing_bits, runtime_bits)
        assert "def Join(self, separator: str, value: List[str]) -> str" in line

    def test_unparseable_falls_back_to_varargs(self):
        typing_bits, runtime_bits = set(), set()
        line = build_method_signature("Weird", {"Definition": "?!"},
                                      typing_bits, runtime_bits)
        assert "*args: Any" in line

    def test_sanitize_identifier(self):
        assert sanitize_identifier("123abc") == "_123abc"
        assert sanitize_identifier("for") == "for_"
        assert sanitize_identifier("a-b c") == "a_b_c"

    def test_safe_class_name(self):
        assert safe_class_name("System.Text.StringBuilder") == "StringBuilder"
        assert safe_class_name("") == "PSObject"


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
        assert "def Read(self, buffer: List[int], offset: int, count: int) -> int: ..." in source
        assert "def proxy_schema(self) -> Dict[str, Any]: ..." in source
        assert "list[Any]" not in source   # py3.8-compatible typing only

    def test_readonly_property_has_no_setter(self):
        source = render_protocol("MyProxy", self.MEMBERS)
        assert "def Length(self, value" not in source


# =============================================================================
# Integration
# =============================================================================

def _real_core_available() -> bool:
    try:
        import virtualshell._core as core
    except ImportError:
        return False
    return not getattr(core, "__vs_stub__", False)


integration = pytest.mark.skipif(
    not (_real_core_available() and shutil.which("pwsh")),
    reason="requires the compiled _core extension and pwsh on PATH",
)


@integration
class TestGenerateEndToEnd:
    @pytest.fixture(scope="class")
    def shell(self):
        from virtualshell import Shell

        sh = Shell(timeout_seconds=60).start()
        yield sh
        sh.stop(force=True)

    def test_generate_for_datetime(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        out_file = tmp_path / "datetime_proxy.py"
        generate(shell, "Get-Date", out_file)
        source = out_file.read_text(encoding="utf-8")

        compile(source, str(out_file), "exec")   # generated file must parse
        assert "class DateTime(Protocol):" in source
        assert "def Year(self) -> int: ..." in source
        assert "def AddDays(self" in source

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
