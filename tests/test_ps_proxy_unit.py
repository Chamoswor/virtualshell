"""Unit tests for ps_proxy helpers (no PowerShell needed)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from virtualshell.ps_proxy import (
    MethodMeta,
    MethodOverload,
    ParamMeta,
    _member_expr,
    _method_call_expr,
    _parse_iso_datetime,
    _parse_schema,
    _ps_quote,
    _static_member_expr,
    _static_method_call_expr,
    build_creation_strategies,
    split_invocation,
    static_type_literal,
)


class TestQuoting:
    def test_plain(self):
        assert _ps_quote("abc") == "'abc'"

    def test_quotes_doubled(self):
        assert _ps_quote("it's") == "'it''s'"

    def test_empty(self):
        assert _ps_quote("") == "''"


class TestExpressions:
    def test_simple_member(self):
        assert _member_expr("$x", "Name") == "$x.Name"

    def test_member_with_special_chars_goes_via_psobject(self):
        assert (_member_expr("$x", "weird name")
                == "$x.PSObject.Properties['weird name'].Value")

    def test_member_with_quote_is_escaped(self):
        assert "''" in _member_expr("$x", "o'brien")

    def test_simple_call(self):
        assert _method_call_expr("$x", "ToString", ["'d'"]) == "$x.ToString('d')"

    def test_call_with_special_name(self):
        expr = _method_call_expr("$x", "my method", ["1", "2"])
        assert expr == "$x.PSObject.Methods['my method'].Invoke(1, 2)"


class TestStaticExpressions:
    def test_static_member(self):
        assert _static_member_expr("$t", "PI") == "$t::PI"

    def test_static_member_with_special_name_uses_reflection(self):
        expr = _static_member_expr("$t", "weird name")
        assert "InvokeMember('weird name'" in expr
        assert "GetProperty,GetField" in expr

    def test_static_call(self):
        assert _static_method_call_expr("$t", "Sqrt", ["16"]) == "$t::Sqrt(16)"

    def test_static_call_with_special_name_uses_reflection(self):
        expr = _static_method_call_expr("$t", "my method", ["1", "2"])
        assert "InvokeMember('my method'" in expr
        assert "InvokeMethod" in expr
        assert "@(1, 2)" in expr


class TestStaticTypeLiteral:
    def test_bare_literal(self):
        assert static_type_literal("[System.Math]") == "System.Math"

    def test_whitespace_tolerated(self):
        assert static_type_literal("  [System.IO.Path] ") == "System.IO.Path"

    def test_generic_literal(self):
        assert (static_type_literal("[System.Collections.Generic.List[int]]")
                == "System.Collections.Generic.List[int]")

    def test_plain_type_name_is_not_a_literal(self):
        assert static_type_literal("System.Math") is None

    def test_invocation_is_not_a_literal(self):
        assert static_type_literal("[System.Text.StringBuilder]::new(3)") is None

    def test_cast_is_not_a_literal(self):
        assert static_type_literal("[int](3)") is None

    def test_variable_is_not_a_literal(self):
        assert static_type_literal("$var") is None

    def test_unbalanced_brackets_rejected(self):
        assert static_type_literal("[int][string]") is None

    def test_empty_and_tiny_inputs(self):
        assert static_type_literal("") is None
        assert static_type_literal("[]") is None


class TestSplitInvocation:
    def test_ctor_like(self):
        assert split_invocation("System.Text.StringBuilder(16)") == (
            "System.Text.StringBuilder", "16")

    def test_no_args(self):
        assert split_invocation("Foo.Bar()") == ("Foo.Bar", "")

    def test_nested_parens(self):
        assert split_invocation("T((1+2), 'x')") == ("T", "(1+2), 'x'")

    def test_not_an_invocation(self):
        assert split_invocation("Foo.Bar") is None
        assert split_invocation("Foo(1).Bar") is None


class TestCreationStrategies:
    def test_variable_ref_is_single_strategy(self):
        strategies = build_creation_strategies("$existing")
        assert strategies == [("variable reference", "$existing")]

    def test_type_name_gets_new_and_new_object(self):
        commands = [cmd for _, cmd in build_creation_strategies("System.Text.StringBuilder")]
        assert "[System.Text.StringBuilder]::new()" in commands
        assert any("New-Object -TypeName 'System.Text.StringBuilder'" in c for c in commands)
        assert any("-ComObject" in c for c in commands)

    def test_ctor_args_preserved(self):
        commands = [cmd for _, cmd in build_creation_strategies("System.Text.StringBuilder(32)")]
        assert "[System.Text.StringBuilder]::new(32)" in commands
        assert any("-ArgumentList 32" in c for c in commands)

    def test_empty_input(self):
        assert build_creation_strategies("  ") == []

    def test_injection_ish_input_still_listed_as_raw_only(self):
        # Not type-like, so no bracketed/new-object variants are fabricated.
        strategies = build_creation_strategies("Get-Date; Remove-Item x")
        labels = [label for label, _ in strategies]
        assert labels == ["raw expression"]


class TestSchemaParsing:
    PAYLOAD = {
        "tn": "System.Text.StringBuilder",
        "props": [
            {"n": "Length", "t": "System.Int32", "w": True},
            {"n": "MaxCapacity", "t": "System.Int32", "w": False},
        ],
        "meths": [
            {"n": "Append", "o": [
                {"r": "System.Text.StringBuilder",
                 "p": [{"n": "value", "t": "System.String",
                        "o": False, "rf": False, "a": False}]},
                {"r": "System.Text.StringBuilder",
                 "p": [{"n": "value", "t": "System.Char",
                        "o": False, "rf": False, "a": False},
                       {"n": "repeatCount", "t": "System.Int32",
                        "o": False, "rf": False, "a": False}]},
            ]},
        ],
        "sm": [{"n": "MyScriptMethod"}],
    }

    def test_full_payload(self):
        schema = _parse_schema(self.PAYLOAD)
        assert schema.type_name == "System.Text.StringBuilder"
        assert schema.properties["Length"].writable is True
        assert schema.properties["MaxCapacity"].writable is False
        assert len(schema.methods["Append"].overloads) == 2
        assert schema.methods["MyScriptMethod"].is_script_method is True

    def test_single_entries_not_wrapped_in_lists(self):
        # ConvertTo-Json flattens single-element arrays to plain objects.
        payload = {
            "tn": "X",
            "props": {"n": "Only", "t": "System.String", "w": False},
            "meths": {"n": "M", "o": {"r": "System.Void", "p": None}},
            "sm": None,
        }
        schema = _parse_schema(payload)
        assert list(schema.properties) == ["Only"]
        assert schema.methods["M"].overloads[0].return_type == "System.Void"

    def test_overload_selection_by_arg_count(self):
        schema = _parse_schema(self.PAYLOAD)
        append = schema.methods["Append"]
        assert len(append.overload_for(1).params) == 1
        assert len(append.overload_for(2).params) == 2
        # No exact match: falls back to the first overload.
        assert append.overload_for(7) is append.overloads[0]

    def test_overload_for_empty(self):
        meta = MethodMeta(name="X")
        assert meta.overload_for(0) is None


class TestIsoDatetimeParsing:
    def test_seven_digit_fraction(self):
        dt = _parse_iso_datetime("2026-03-05T10:20:30.1234567+01:00")
        assert dt.microsecond == 123456
        assert dt.utcoffset() == timedelta(hours=1)

    def test_zulu_suffix(self):
        dt = _parse_iso_datetime("2026-03-05T10:20:30.5000000Z")
        assert dt.tzinfo == timezone.utc

    def test_no_fraction(self):
        assert _parse_iso_datetime("2026-03-05T10:20:30") == datetime(2026, 3, 5, 10, 20, 30)
