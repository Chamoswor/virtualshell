"""Unit tests for the module-level helpers in virtualshell.shell."""
from __future__ import annotations

import pytest

from virtualshell.errors import ExecutionError, ExecutionTimeoutError
from virtualshell.shell import (
    ExitCode,
    _effective_timeout,
    _raise_on_failure,
    _strip_result_fields,
    quote_pwsh_literal,
)

from conftest import FakeExecutionResult


class TestQuotePwshLiteral:
    def test_empty_string(self):
        assert quote_pwsh_literal("") == "''"

    def test_plain_text(self):
        assert quote_pwsh_literal("hello") == "'hello'"

    def test_single_quotes_are_doubled(self):
        assert quote_pwsh_literal("it's") == "'it''s'"
        assert quote_pwsh_literal("''") == "''''''"

    def test_interpolation_characters_left_untouched(self):
        assert quote_pwsh_literal("$env:PATH `n \"x\"") == "'$env:PATH `n \"x\"'"

    def test_unicode_passthrough(self):
        assert quote_pwsh_literal("æøå") == "'æøå'"


class TestEffectiveTimeout:
    def test_user_timeout_wins(self):
        assert _effective_timeout(2.5, 30.0) == 2.5

    def test_none_falls_back_to_default(self):
        assert _effective_timeout(None, 30.0) == 30.0

    def test_zero_falls_back_to_default(self):
        assert _effective_timeout(0, 30.0) == 30.0

    def test_negative_falls_back_to_default(self):
        assert _effective_timeout(-1, 30.0) == 30.0

    def test_no_timeout_at_all_gives_zero(self):
        assert _effective_timeout(None, 0.0) == 0.0
        assert _effective_timeout(None, None) == 0.0  # type: ignore[arg-type]


class TestStripResultFields:
    def test_strips_string_fields(self):
        res = FakeExecutionResult(out="  hi \r\n", err="\tboom\n")
        stripped = _strip_result_fields(res)
        assert stripped.out == "hi"
        assert stripped.err == "boom"

    def test_leaves_non_string_fields_alone(self):
        res = FakeExecutionResult(out=None, err=42)
        stripped = _strip_result_fields(res)
        assert stripped.out is None
        assert stripped.err == 42


class TestRaiseOnFailure:
    def test_success_is_noop(self):
        res = FakeExecutionResult(success=True)
        _raise_on_failure(res, raise_on_error=True, label="X", timeout_used=1.0)

    def test_timeout_raises_timeout_error(self):
        res = FakeExecutionResult(err="Command timed out", exit_code=-1, success=False)
        with pytest.raises(ExecutionTimeoutError, match="timed out after 1.5s"):
            _raise_on_failure(res, raise_on_error=False, label="Command", timeout_used=1.5)

    def test_timeout_not_raised_when_disabled(self):
        res = FakeExecutionResult(err="Command timed out", exit_code=-1, success=False)
        # raise_on_timeout=False and raise_on_error=False -> silent
        _raise_on_failure(res, raise_on_error=False, raise_on_timeout=False,
                          label="Command", timeout_used=1.5)
        # raise_on_timeout=False but raise_on_error=True -> generic ExecutionError
        with pytest.raises(ExecutionError):
            _raise_on_failure(res, raise_on_error=True, raise_on_timeout=False,
                              label="Command", timeout_used=1.5)

    def test_failure_raises_execution_error_with_backend_message(self):
        res = FakeExecutionResult(err="boom", exit_code=1, success=False)
        with pytest.raises(ExecutionError, match="boom"):
            _raise_on_failure(res, raise_on_error=True, label="Command", timeout_used=None)

    def test_failure_without_message_uses_label_and_exit_code(self):
        res = FakeExecutionResult(err="", exit_code=7, success=False)
        with pytest.raises(ExecutionError, match="Script failed with exit_code=7"):
            _raise_on_failure(res, raise_on_error=True, label="Script", timeout_used=None)

    def test_failure_is_silent_without_raise_on_error(self):
        res = FakeExecutionResult(err="boom", exit_code=1, success=False)
        _raise_on_failure(res, raise_on_error=False, label="Command", timeout_used=None)


class TestExitCode:
    def test_values(self):
        assert ExitCode.SUCCESS == 0
        assert ExitCode.GENERAL_ERROR == 1
        assert ExitCode.TIMEOUT == -1
        assert ExitCode.RESTARTING == -2
        assert ExitCode.NOT_RUNNING == -3

    def test_reexported_from_package(self):
        import virtualshell
        assert virtualshell.ExitCode is ExitCode
