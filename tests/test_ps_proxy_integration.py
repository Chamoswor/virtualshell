"""Integration tests for the reworked PsProxy against a real PowerShell host.

Covers scalar fidelity, byte[] transfers via the zero-copy bridge,
out-buffers, sub-proxies, property writes, error paths and the schema API.
Runs once per installed edition (pwsh and Windows PowerShell 5.1).
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from virtualshell.errors import ExecutionError
from virtualshell.ps_object import PSObject

from conftest import integration

pytestmark = integration


@pytest.fixture(scope="module")
def shell(edition):
    from virtualshell import Shell

    sh = Shell(timeout_seconds=60, powershell_edition=edition).start()
    yield sh
    sh.stop(force=True)


class TestCreationAndBinding:
    def test_create_from_type_name(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        assert sb.type_name == "System.Text.StringBuilder"
        assert sb.ps_ref.startswith("$__vs_proxy_")

    def test_create_with_ctor_args(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder(32)")
        assert sb.Capacity == 32

    def test_bind_existing_variable(self, shell):
        shell.run("$vs_bind_target = [System.Text.StringBuilder]::new()",
                  raise_on_error=True)
        sb = shell.make_proxy("", "$vs_bind_target")
        sb.Append("bundet")
        assert shell.run("$vs_bind_target.ToString()").out.strip() == "bundet"

    def test_bind_null_variable_raises(self, shell):
        with pytest.raises(ValueError, match="null or undefined"):
            shell.make_proxy("", "$vs_definitely_not_set_qq")

    def test_unknown_type_raises_with_strategy_report(self, shell):
        with pytest.raises(RuntimeError, match="Failed to create"):
            shell.make_proxy("", "No.Such.Type.Exists")

    def test_release_removes_owned_variable(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        ref = sb.ps_ref
        sb.release()
        res = shell.run(f"if ($null -eq {ref}) {{ 'gone' }} else {{ 'alive' }}")
        assert res.out.strip() == "gone"


class TestScalars:
    def test_string_property_and_method(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        sb.Append("hei fra proxy: æøå ✓")
        text = sb.ToString()
        assert isinstance(text, str)
        assert text == "hei fra proxy: æøå ✓"

    def test_string_with_newlines_survives(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        sb.Append("linje1\nlinje2\r\nlinje3")
        assert sb.ToString() == "linje1\nlinje2\r\nlinje3"

    def test_int_property(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        sb.Append("1234")
        length = sb.Length
        assert isinstance(length, int) and length == 4

    def test_bool_return(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        assert sb.Equals(sb) is True

    def test_parameterized_property_as_method(self, shell):
        # StringBuilder.Chars is an indexer (ParameterizedProperty): callable
        # with an index argument, like in PowerShell ($sb.Chars(1)).
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        sb.Append("Hei")
        assert sb.Chars(1) == "e"
        assert "Chars" in dir(sb)

    def test_datetime_values(self, shell):
        shell.run("$vs_dt = [datetime]::new(2024, 3, 5, 10, 20, 30)",
                  raise_on_error=True)
        p = shell.make_proxy("", "$vs_dt")
        assert p.Year == 2024
        assert p.DayOfWeek == "Tuesday"      # enum -> string
        d = p.Date
        assert isinstance(d, datetime) and d == datetime(2024, 3, 5)
        added = p.AddDays(1)
        assert isinstance(added, datetime) and added.day == 6

    def test_timespan_becomes_timedelta(self, shell):
        shell.run("$vs_dt_a = [datetime]::new(2024, 1, 2)", raise_on_error=True)
        p = shell.make_proxy("", "$vs_dt_a")
        delta = p.Subtract(datetime(2024, 1, 1))
        assert isinstance(delta, timedelta)
        assert delta == timedelta(days=1)

    def test_datetime_argument_marshalling(self, shell):
        shell.run("$vs_dt_b = [datetime]::new(2024, 6, 1)", raise_on_error=True)
        p = shell.make_proxy("", "$vs_dt_b")
        assert p.CompareTo(datetime(2024, 1, 1)) == 1

    def test_null_property_is_none(self, shell):
        shell.run("$vs_nullprop = [pscustomobject]@{ Empty = $null }",
                  raise_on_error=True)
        p = shell.make_proxy("", "$vs_nullprop")
        assert p.Empty is None

    def test_float_return(self, shell):
        shell.run("$vs_ts = [timespan]::FromSeconds(90)", raise_on_error=True)
        p = shell.make_proxy("", "$vs_ts")
        assert p.TotalMinutes == pytest.approx(1.5)


class TestBytesViaBridge:
    def test_byte_array_argument_and_return(self, shell):
        ms = shell.make_proxy("", "System.IO.MemoryStream")
        payload = b"hello bridge \x00\x01\xfe\xff" * 100
        ms.Write(payload, 0, len(payload))
        result = ms.ToArray()
        assert isinstance(result, bytes)
        assert result == payload

    def test_out_buffer_bytearray(self, shell):
        ms = shell.make_proxy("", "System.IO.MemoryStream")
        ms.Write(b"hello world", 0, 11)
        ms.Position = 0

        buffer = bytearray(5)
        read = ms.Read(buffer, 0, 5)
        assert read == 5
        assert bytes(buffer) == b"hello"

    def test_property_write_and_scalar_roundtrip(self, shell):
        ms = shell.make_proxy("", "System.IO.MemoryStream")
        ms.Write(b"0123456789", 0, 10)
        ms.Position = 3          # writable property through the proxy
        assert ms.Position == 3
        assert ms.ReadByte() == ord("3")

    def test_setlength_truncates(self, shell):
        ms = shell.make_proxy("", "System.IO.MemoryStream")
        ms.Write(b"abcdef", 0, 6)
        ms.SetLength(4)
        assert ms.ToArray() == b"abcd"

    def test_large_payload_multi_chunk(self, shell):
        import os
        ms = shell.make_proxy("", "System.IO.MemoryStream")
        payload = os.urandom(2 * 1024 * 1024)  # > default proxy chunk size
        ms.Write(payload, 0, len(payload))
        assert ms.ToArray() == payload


class TestSubProxies:
    def test_complex_return_becomes_proxy(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        appended = sb.Append("abc")
        # StringBuilder.Append returns the same builder: a live sub-proxy.
        assert appended.type_name == "System.Text.StringBuilder"
        appended.Append("def")
        assert sb.ToString() == "abcdef"

    def test_complex_property_becomes_proxy(self, shell):
        shell.run(
            "$vs_nested = [pscustomobject]@{ "
            "Inner = [System.Text.StringBuilder]::new() }",
            raise_on_error=True)
        p = shell.make_proxy("", "$vs_nested")
        inner = p.Inner
        inner.Append("nested!")
        assert shell.run("$vs_nested.Inner.ToString()").out.strip() == "nested!"

    def test_proxy_as_argument(self, shell):
        a = shell.make_proxy("", "System.Text.StringBuilder")
        a.Append("A")
        b = shell.make_proxy("", "System.Text.StringBuilder")
        # Equals(proxy) marshals the other proxy by its $ref.
        assert a.Equals(b) is False
        assert a.Equals(a) is True


class TestPsCustomObjects:
    @pytest.fixture()
    def custom(self, shell):
        shell.run(
            "$vs_custom = [pscustomobject]@{ Navn = 'æøå'; Tall = 7 }; "
            "$vs_custom | Add-Member -MemberType ScriptMethod -Name Hilsen "
            "-Value { \"hei $($this.Navn)\" } -Force",
            raise_on_error=True)
        return shell.make_proxy("", "$vs_custom")

    def test_note_properties(self, custom):
        assert custom.Navn == "æøå"
        assert custom.Tall == 7

    def test_note_property_write(self, custom, shell):
        custom.Navn = "nytt navn"
        assert shell.run("$vs_custom.Navn").out.strip() == "nytt navn"

    def test_script_method(self, custom):
        custom.Navn = "æøå"
        assert custom.Hilsen() == "hei æøå"

    def test_to_psobject_snapshot(self, custom):
        custom.Navn = "æøå"
        snapshot = custom.to_psobject()
        assert isinstance(snapshot, PSObject)
        assert snapshot["Navn"] == "æøå"
        assert snapshot["Tall"] == 7


class TestErrors:
    def test_unknown_attribute(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        with pytest.raises(AttributeError, match="NoSuchMember"):
            sb.NoSuchMember

    def test_method_error_surfaces(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        with pytest.raises(ExecutionError):
            sb.Insert(9999, "x")   # ArgumentOutOfRangeException

    def test_read_only_property_write_rejected(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        with pytest.raises(AttributeError, match="read-only"):
            sb.MaxCapacity = 5

    def test_overwriting_method_rejected(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        with pytest.raises(AttributeError, match="method"):
            sb.Append = "nope"

    def test_kwargs_rejected(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        with pytest.raises(TypeError, match="keyword"):
            sb.Append(value="x")

    def test_dynamic_attributes_allowed(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        sb.my_python_note = 42       # not a PS member -> stored locally
        assert sb.my_python_note == 42


class TestSchemaAndIntrospection:
    def test_dir_contains_members(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        names = dir(sb)
        assert "Append" in names
        assert "Length" in names
        assert "proxy_schema" in names

    def test_proxy_schema_shape(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        schema = sb.proxy_schema()
        assert schema["TypeName"] == "System.Text.StringBuilder"
        method_names = {m["Name"] for m in schema["Methods"]}
        assert "Append" in method_names
        props = {p["Name"]: p for p in schema["Properties"]}
        assert props["MaxCapacity"]["Writable"] is False

    def test_schema_is_cached_per_type(self, shell):
        from virtualshell.ps_proxy import _SCHEMA_CACHE
        a = shell.make_proxy("", "System.Text.StringBuilder")
        key = (shell.python_run_id, "System.Text.StringBuilder")
        assert key in _SCHEMA_CACHE

    def test_repr(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        assert "System.Text.StringBuilder" in repr(sb)


class TestMultiCall:
    def test_repeat_count(self, shell):
        al = shell.make_proxy("", "System.Collections.ArrayList")
        results = al.proxy_multi_call(al.Add, ["a", "b", "c"])
        assert results == [0, 1, 2]
        assert al.Count == 3

    def test_non_method_rejected(self, shell):
        al = shell.make_proxy("", "System.Collections.ArrayList")
        with pytest.raises(TypeError):
            al.proxy_multi_call(len, 3)


class TestStaticProxies:
    def test_type_literal_selects_static_mode(self, shell):
        m = shell.make_proxy("", "[System.Math]")
        assert m.type_name == "System.Math"
        assert "static" in repr(m)
        assert m.Sqrt(16.0) == 4.0
        assert m.Max(3, 7) == 7

    def test_constant_field_reads_as_property(self, shell):
        m = shell.make_proxy("", "[System.Math]")
        assert abs(m.PI - 3.141592653589793) < 1e-12

    def test_static_method_with_string_args(self, shell):
        p = shell.make_proxy("", "[System.IO.Path]")
        assert p.Combine("a", "b") in ("a\\b", "a/b")

    def test_static_datetime_roundtrip(self, shell):
        from datetime import datetime

        dt = shell.make_proxy("", "[datetime]")
        parsed = dt.Parse("2024-01-02T03:04:05")
        assert parsed == datetime(2024, 1, 2, 3, 4, 5)

    def test_enum_values_read_as_strings(self, shell):
        dow = shell.make_proxy("", "[System.DayOfWeek]")
        assert dow.Monday == "Monday"

    def test_writable_static_property_and_const(self, shell):
        shell.run(
            "Add-Type -TypeDefinition 'public static class VsStaticTest { "
            "public static int Counter { get; set; } "
            "public static string Echo(string s) { return s + \"!\"; } "
            "public const double Ratio = 2.5; }'",
            raise_on_error=True)
        t = shell.make_proxy("", "[VsStaticTest]")
        t.Counter = 5
        assert t.Counter == 5
        assert t.Echo("hei") == "hei!"
        assert t.Ratio == 2.5
        with pytest.raises(AttributeError, match="read-only"):
            t.Ratio = 3.0

    def test_static_true_with_unbracketed_name(self, shell):
        m = shell.make_proxy("", "System.Math", static=True)
        assert m.Sqrt(9.0) == 3.0

    def test_static_true_binds_existing_type_variable(self, shell):
        shell.run("$vs_static_t = [System.Math]", raise_on_error=True)
        m = shell.make_proxy("", "$vs_static_t", static=True)
        assert m.Sqrt(25.0) == 5.0

    def test_static_true_rejects_non_type_variable(self, shell):
        shell.run("$vs_not_a_type = 42", raise_on_error=True)
        with pytest.raises(ValueError, match="does not hold"):
            shell.make_proxy("", "$vs_not_a_type", static=True)

    def test_unknown_static_type_raises(self, shell):
        with pytest.raises(RuntimeError, match="Failed to resolve static type"):
            shell.make_proxy("", "[No.Such.Type.Exists]")

    def test_instance_members_absent_from_static_schema(self, shell):
        m = shell.make_proxy("", "[System.Text.StringBuilder]")
        names = {x["Name"] for x in m.proxy_schema()["Methods"]}
        # ToString is an instance method; a static proxy must not offer it.
        assert "ToString" not in names

    def test_static_and_instance_schemas_coexist(self, shell):
        from virtualshell.ps_proxy import _SCHEMA_CACHE
        shell.make_proxy("", "[System.Text.StringBuilder]")
        shell.make_proxy("", "System.Text.StringBuilder")
        run_id = shell.python_run_id
        assert (run_id, "System.Text.StringBuilder") in _SCHEMA_CACHE
        assert (run_id, "static:System.Text.StringBuilder") in _SCHEMA_CACHE

    def test_static_multi_call(self, shell):
        m = shell.make_proxy("", "[System.Math]")
        assert m.proxy_multi_call(m.Sqrt, [4.0, 9.0, 16.0]) == [2.0, 3.0, 4.0]

    def test_messagebox_scenario_schema(self, shell):
        """The motivating case: a WinForms MessageBox static proxy exposes
        Show without being called (calling it would block on a modal)."""
        res = shell.run("Add-Type -AssemblyName System.Windows.Forms")
        if not res.success:
            pytest.skip("System.Windows.Forms not available")
        mb = shell.make_proxy("", "[System.Windows.Forms.MessageBox]")
        assert mb.type_name == "System.Windows.Forms.MessageBox"
        names = {x["Name"] for x in mb.proxy_schema()["Methods"]}
        assert "Show" in names


def _load_module(path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestProvenance:
    """Proxies remember how they were reached (ps_origin); stubs embed it."""

    def test_root_origins(self, shell):
        created = shell.make_proxy("", "System.Text.StringBuilder(32)")
        assert created.ps_origin == "[System.Text.StringBuilder]::new(32)"
        shell.run("$vs_prov_root = [System.Text.StringBuilder]::new()", raise_on_error=True)
        bound = shell.make_proxy("", "$vs_prov_root")
        assert bound.ps_origin == "$vs_prov_root"
        static = shell.make_proxy("", "[System.Math]")
        assert static.ps_origin == "[System.Math]"

    def test_derived_origins_follow_members_and_calls(self, shell):
        shell.run("$vs_prov_root = [System.Text.StringBuilder]::new()", raise_on_error=True)
        root = shell.make_proxy("", "$vs_prov_root")
        assert root.Append("x").ps_origin == "$vs_prov_root.Append('x')"
        assert root.Append("y").Append(3).ps_origin == "($vs_prov_root.Append('y')).Append(3)"

        created = shell.make_proxy("", "System.Text.StringBuilder(8)")
        assert created.Append("z").ps_origin == \
            "([System.Text.StringBuilder]::new(8)).Append('z')"

        shell.run("$vs_prov_dir = [System.IO.DirectoryInfo]::new("
                  "[System.IO.Path]::GetTempPath())", raise_on_error=True)
        d = shell.make_proxy("", "$vs_prov_dir")
        assert d.Root.ps_origin == "$vs_prov_dir.Root"

    def test_origin_is_none_when_arguments_only_exist_as_temporaries(self, shell):
        shell.run("$vs_prov_root = [System.Text.StringBuilder]::new()", raise_on_error=True)
        root = shell.make_proxy("", "$vs_prov_root")
        assert root.Append(b"\x01\x02").ps_origin is None      # bytes go via the bridge

    def test_derived_expression_is_evaluated_once_into_own_variable(self, shell):
        shell.run("$vs_prov_root = [System.Text.StringBuilder]::new()", raise_on_error=True)
        derived = shell.make_proxy("", "$vs_prov_root.Append('q')")
        assert derived.ps_ref.startswith("$__vs_proxy_")
        assert derived.ps_origin == "$vs_prov_root.Append('q')"
        _ = derived.Length; _ = derived.Length                  # no re-evaluation
        assert shell.run("$vs_prov_root.ToString()").out.strip() == "q"

    def test_generate_from_derived_proxy_embeds_origin(self, shell, tmp_path):
        from virtualshell.generate_psobject import generate

        shell.run("$vs_prov_root = [System.Text.StringBuilder]::new()", raise_on_error=True)
        root = shell.make_proxy("", "$vs_prov_root")
        derived = root.Append("ab")

        out = tmp_path / "Derived.py"
        generate(shell, derived, out)                            # the proxy itself
        proto = _load_module(out).StringBuilder
        assert proto.__ps_expression__ == "$vs_prov_root.Append('ab')"

        out2 = tmp_path / "Derived2.py"
        generate(shell, derived.ps_ref, out2)                    # its variable name
        assert _load_module(out2).StringBuilder.__ps_expression__ == \
            "$vs_prov_root.Append('ab')"

        # Wherever $vs_prov_root exists, the stub reaches the object again.
        again = shell.make_proxy(proto)
        assert again.ps_ref != derived.ps_ref
        assert again.ToString() == "abab"

    def test_stale_temporary_gives_actionable_error(self, shell):
        with pytest.raises(ValueError, match="another session"):
            shell.make_proxy("", "$__vs_ret_999999")


class TestStaticMembersViaInstance:
    """Static .NET members are reachable on instance proxies, as in Python."""

    def test_static_methods_and_members_on_datetime(self, shell):
        shell.run("$vs_stat_dt = [datetime]::new(2024, 2, 10)", raise_on_error=True)
        p = shell.make_proxy("", "$vs_stat_dt")
        names = {m["Name"] for m in p.proxy_schema()["Methods"]}
        assert "IsLeapYear" in names and "AddDays" in names      # static and instance
        assert "ReferenceEquals" not in names                     # Object's statics skipped
        assert p.IsLeapYear(2024) is True
        assert p.DaysInMonth(2024, 2) == 29
        assert isinstance(p.UtcNow, datetime)                     # static property
        assert p.MaxValue.year == 9999                            # static readonly field
        assert p.Year == 2024                                     # instance still first

    def test_static_object_result_carries_origin(self, shell):
        shell.run("$vs_stat_enc = [System.Text.Encoding]::UTF8", raise_on_error=True)
        enc = shell.make_proxy("", "$vs_stat_enc")
        ascii_enc = enc.GetEncoding("ascii")                     # static, inherited
        assert ascii_enc.WebName == "us-ascii"
        assert ascii_enc.ps_origin == "($vs_stat_enc.GetType())::GetEncoding('ascii')"


class TestCollectionProtocol:
    """.NET collections behind a proxy behave like Python sequences/mappings."""

    def test_generic_list_is_a_sequence(self, shell):
        shell.run("$vs_col_list = [System.Collections.Generic.List[string]]::new(); "
                  "$vs_col_list.Add('a'); $vs_col_list.Add('b'); $vs_col_list.Add('c')",
                  raise_on_error=True)
        p = shell.make_proxy("", "$vs_col_list")
        assert len(p) == 3
        assert p[0] == "a" and p[-1] == "c"
        assert p[1:] == ["b", "c"]
        assert list(p) == ["a", "b", "c"]
        assert "b" in p and "z" not in p
        with pytest.raises(IndexError):
            _ = p[3]
        assert bool(p) is True

    def test_readonly_collection_and_array(self, shell):
        shell.run("$vs_col_ro = [System.Collections.ObjectModel.ReadOnlyCollection[int]]::new("
                  "[int[]](1, 2, 3))", raise_on_error=True)
        ro = shell.make_proxy("", "$vs_col_ro")
        assert len(ro) == 3 and ro[1] == 2 and list(ro) == [1, 2, 3] and 2 in ro

        shell.run("$vs_col_arr = [string[]]('x', 'y')", raise_on_error=True)
        arr = shell.make_proxy("", "$vs_col_arr")     # Length, no Item indexer
        assert len(arr) == 2 and arr[1] == "y" and list(arr) == ["x", "y"]

    def test_dictionary_is_a_mapping(self, shell):
        shell.run("$vs_col_dict = [System.Collections.Generic.Dictionary[string,int]]::new(); "
                  "$vs_col_dict.Add('one', 1); $vs_col_dict.Add('two', 2)", raise_on_error=True)
        d = shell.make_proxy("", "$vs_col_dict")
        assert len(d) == 2
        assert d["two"] == 2
        assert sorted(d) == ["one", "two"]             # iterates keys
        assert "one" in d and "three" not in d

    def test_elements_that_are_objects_become_proxies_with_origin(self, shell):
        shell.run("$vs_col_objs = [System.Collections.Generic.List[System.Text.StringBuilder]]::new(); "
                  "$vs_col_objs.Add([System.Text.StringBuilder]::new('p')); "
                  "$vs_col_objs.Add([System.Text.StringBuilder]::new('q'))", raise_on_error=True)
        col = shell.make_proxy("", "$vs_col_objs")
        items = list(col)
        assert [i.ToString() for i in items] == ["p", "q"]
        assert items[1].ps_origin == "$vs_col_objs.Item(1)"

    def test_proxy_select_bulk_read(self, shell):
        shell.run("$vs_sel = [System.Collections.Generic.List[object]]::new(); "
                  "$vs_sel.Add([pscustomobject]@{ Name='a'; When=[datetime]::new(2024,1,2); "
                  "Kind=[System.DayOfWeek]::Monday; Inner=[System.Text.StringBuilder]::new('x') }); "
                  "$vs_sel.Add([pscustomobject]@{ Name='b'; When=$null; Kind=[System.DayOfWeek]::Friday; "
                  "Inner=$null })", raise_on_error=True)
        col = shell.make_proxy("", "$vs_sel")
        rows = col.proxy_select("Name", "Kind", when="When", inner="Inner.ToString()",
                                length="Name.Length")
        assert rows == [
            {"Name": "a", "Kind": "Monday", "when": "2024-01-02T00:00:00.0000000",
             "inner": "x", "length": 1},
            {"Name": "b", "Kind": "Friday", "when": None, "inner": None, "length": 1},
        ]
        shell.run("$vs_sel_empty = [System.Collections.Generic.List[object]]::new()",
                  raise_on_error=True)
        assert shell.make_proxy("", "$vs_sel_empty").proxy_select("Name") == []
        sb = shell.make_proxy("", "System.Text.StringBuilder('solo')")
        assert sb.proxy_select("Length") == [{"Length": 4}]          # non-collection: one row
        with pytest.raises(ValueError):
            col.proxy_select()

    def test_generic_on_static_type_proxy(self, shell):
        arr = shell.make_proxy("", "[System.Array]")
        empty = arr.generic("Empty", "System.Int32")()                 # [Array]::Empty[int]()
        assert len(empty) == 0
        with pytest.raises(TypeError):
            shell.make_proxy("", "System.Text.StringBuilder").generic("Append", 3)

    def test_read_error_is_reported_in_one_round_trip(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        with pytest.raises(ExecutionError, match="Read property"):
            sb.Chars(99)     # ArgumentOutOfRange inside the merged assign+read command

    def test_non_collection_raises_but_is_truthy(self, shell):
        sb = shell.make_proxy("", "System.Text.StringBuilder")
        assert bool(sb) is True
        with pytest.raises(TypeError, match="not a collection"):
            len(sb)
        with pytest.raises(TypeError):
            iter(sb)
