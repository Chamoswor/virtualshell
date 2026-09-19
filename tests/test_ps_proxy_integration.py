"""Integration tests for the reworked PsProxy against real pwsh.

Covers scalar fidelity, byte[] transfers via the zero-copy bridge,
out-buffers, sub-proxies, property writes, error paths and the schema API.
"""
from __future__ import annotations

import shutil
from datetime import datetime, timedelta

import pytest

from virtualshell.errors import ExecutionError
from virtualshell.ps_object import PSObject


def _real_core_available() -> bool:
    try:
        import virtualshell._core as core
    except ImportError:
        return False
    return not getattr(core, "__vs_stub__", False)


pytestmark = pytest.mark.skipif(
    not (_real_core_available() and shutil.which("pwsh")),
    reason="requires the compiled _core extension and pwsh on PATH",
)


@pytest.fixture(scope="module")
def shell():
    from virtualshell import Shell

    sh = Shell(timeout_seconds=60).start()
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
