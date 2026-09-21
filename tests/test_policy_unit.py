"""Unit tests for ExecutionPolicy: the pure decision logic, the extraction
script plumbing, and the Shell wiring (with extraction stubbed out so no real
PowerShell is needed)."""
from __future__ import annotations

import pytest

from virtualshell.errors import PolicyViolationError, VirtualShellError
from virtualshell.policy import (
    DYNAMIC,
    ConfirmRequest,
    ExecutionPolicy,
    build_extraction_script,
    parse_extraction_output,
)
from virtualshell.shell import Shell

from conftest import FakeExecutionResult


class TestInspectDenyAllow:
    def test_no_rules_allows_everything(self):
        p = ExecutionPolicy()
        d = p.inspect("Remove-Item x", ["Remove-Item"])
        assert d.allowed and d.command == "Remove-Item x"

    def test_deny_blocks_matching(self):
        p = ExecutionPolicy(deny=["Stop-Computer", "Remove-*"])
        d = p.inspect("Remove-Item x", ["Remove-Item"])
        assert not d.allowed
        assert "deny" in d.reason
        assert d.matched == ("Remove-Item",)

    def test_deny_is_case_insensitive(self):
        p = ExecutionPolicy(deny=["remove-*"])
        assert not p.inspect("x", ["REMOVE-ITEM"]).allowed

    def test_allow_list_blocks_others(self):
        p = ExecutionPolicy(allow=["Get-*", "Select-Object"])
        assert p.inspect("x", ["Get-Date", "Select-Object"]).allowed
        d = p.inspect("x", ["Get-Date", "Set-Content"])
        assert not d.allowed and "allow list" in d.reason

    def test_deny_wins_over_allow(self):
        p = ExecutionPolicy(allow=["*"], deny=["Stop-*"])
        assert not p.inspect("x", ["Stop-Process"]).allowed

    def test_no_commands_always_allowed(self):
        p = ExecutionPolicy(read_only=True, allow=[], deny=["*"])
        assert p.inspect("$x = 5", []).allowed


class TestInspectReadOnly:
    def test_safe_commands_pass(self):
        p = ExecutionPolicy(read_only=True)
        names = ["Get-ChildItem", "Where-Object", "Select-Object",
                 "Measure-Object", "ConvertTo-Json"]
        assert p.inspect("x", names).allowed

    def test_mutating_command_blocked(self):
        p = ExecutionPolicy(read_only=True)
        d = p.inspect("x", ["Get-Item", "Set-Content"])
        assert not d.allowed
        assert "read-only" in d.reason and d.matched == ("Set-Content",)

    def test_allow_extends_the_lane(self):
        p = ExecutionPolicy(read_only=True, allow=["Import-Csv"])
        assert p.inspect("x", ["Import-Csv"]).allowed
        assert not p.inspect("x", ["Import-Clixml"]).allowed

    def test_external_program_blocked(self):
        p = ExecutionPolicy(read_only=True)
        assert not p.inspect("x", ["C:\\tools\\evil.exe"]).allowed


class TestInspectDynamic:
    def test_dynamic_blocked_when_read_only(self):
        p = ExecutionPolicy(read_only=True)
        d = p.inspect("& $cmd", [DYNAMIC])
        assert not d.allowed and "dynamic" in d.reason

    def test_dynamic_blocked_when_allow_list(self):
        p = ExecutionPolicy(allow=["Get-*"])
        assert not p.inspect("& $cmd", [DYNAMIC]).allowed

    def test_invoke_expression_counts_as_dynamic(self):
        p = ExecutionPolicy(read_only=True)
        d = p.inspect("iex $s", ["Invoke-Expression"])
        assert not d.allowed and "dynamic" in d.reason

    def test_dynamic_allowed_when_policy_lenient(self):
        p = ExecutionPolicy(deny=["Stop-*"])
        assert p.inspect("& $cmd", [DYNAMIC]).allowed

    def test_block_dynamic_override(self):
        assert not ExecutionPolicy(block_dynamic=True).inspect("x", [DYNAMIC]).allowed
        assert ExecutionPolicy(read_only=True, block_dynamic=False,
                               ).inspect("x", [DYNAMIC]).allowed


class TestInspectConfirm:
    def test_confirm_without_handler_blocks(self):
        p = ExecutionPolicy(confirm=["Remove-*"])
        d = p.inspect("Remove-Item x", ["Remove-Item"])
        assert not d.allowed and "on_confirm" in d.reason

    def test_confirm_approved_runs_for_real(self):
        seen = []

        def approve(req: ConfirmRequest) -> bool:
            seen.append(req)
            return True

        p = ExecutionPolicy(confirm=["Remove-*"], on_confirm=approve,
                            dry_run_destructive=True)
        d = p.inspect("Remove-Item x", ["Remove-Item"])
        assert d.allowed
        assert d.command == "Remove-Item x"  # approval skips the WhatIf wrap
        assert not d.transformed
        assert seen[0].matched == ("Remove-Item",)
        assert seen[0].command == "Remove-Item x"

    def test_confirm_denied_blocks(self):
        p = ExecutionPolicy(confirm=["Remove-*"], on_confirm=lambda r: False)
        d = p.inspect("Remove-Item x", ["Remove-Item"])
        assert not d.allowed and "denied" in d.reason

    def test_unmatched_commands_skip_confirmation(self):
        called = []
        p = ExecutionPolicy(confirm=["Remove-*"],
                            on_confirm=lambda r: called.append(r) or True)
        assert p.inspect("Get-Date", ["Get-Date"]).allowed
        assert called == []


class TestInspectDryRun:
    def test_destructive_gets_whatif_wrap(self):
        p = ExecutionPolicy(dry_run_destructive=True)
        d = p.inspect("Remove-Item x", ["Remove-Item"])
        assert d.allowed and d.transformed
        assert d.command == "& { $WhatIfPreference = $true; Remove-Item x }"

    def test_non_destructive_untouched(self):
        p = ExecutionPolicy(dry_run_destructive=True)
        d = p.inspect("Get-Date", ["Get-Date"])
        assert d.allowed and not d.transformed and d.command == "Get-Date"

    def test_custom_destructive_patterns(self):
        p = ExecutionPolicy(dry_run_destructive=True, destructive=["Invoke-Nuke"])
        assert p.inspect("Remove-Item x", ["Remove-Item"]).transformed is False
        assert p.inspect("Invoke-Nuke", ["Invoke-Nuke"]).transformed is True


class TestExtractionPlumbing:
    def test_script_embeds_command_as_literal(self):
        script, beg, end = build_extraction_script("Get-Date; 'it''s'")
        assert "ParseInput" in script
        assert "'Get-Date; ''it''''s'''" in script  # single-quote doubling
        assert beg in script and end in script

    def test_parse_roundtrip(self):
        out = f"noise\n<<B>>Get-Date\x01Remove-Item<<E>>\ntail"
        assert parse_extraction_output(out, "<<B>>", "<<E>>") == (
            "Get-Date", "Remove-Item")

    def test_parse_empty_payload(self):
        assert parse_extraction_output("<<B>><<E>>", "<<B>>", "<<E>>") == ()

    def test_parse_missing_markers_fails_closed(self):
        with pytest.raises(VirtualShellError, match="policy inspection failed"):
            parse_extraction_output("garbage", "<<B>>", "<<E>>")


class TestShellWiring:
    """Shell-level enforcement with the parser round trip stubbed out."""

    @staticmethod
    def _stub_names(sh: Shell, mapping):
        sh._command_names = lambda cmd: tuple(mapping.get(cmd, ()))  # type: ignore

    def test_run_blocked_raises_and_never_executes(self, fake_core):
        sh = Shell(policy=ExecutionPolicy(deny=["Remove-*"]), cpp_module=fake_core).start()
        self._stub_names(sh, {"Remove-Item x": ["Remove-Item"]})
        calls_before = len(fake_core.last_shell.calls)
        with pytest.raises(PolicyViolationError) as ei:
            sh.run("Remove-Item x")
        assert ei.value.command == "Remove-Item x"
        assert ei.value.matched == ("Remove-Item",)
        assert len(fake_core.last_shell.calls) == calls_before  # nothing executed

    def test_run_allowed_passes_through(self, fake_core):
        sh = Shell(policy=ExecutionPolicy(deny=["Remove-*"]), cpp_module=fake_core).start()
        self._stub_names(sh, {"Get-Date": ["Get-Date"]})
        assert sh.run("Get-Date").out == "Get-Date"

    def test_batch_validated_before_any_execution(self, fake_core):
        sh = Shell(policy=ExecutionPolicy(deny=["Bad-*"]), cpp_module=fake_core).start()
        self._stub_names(sh, {"ok": ["Get-Date"], "Bad-Thing": ["Bad-Thing"]})
        calls_before = len(fake_core.last_shell.calls)
        with pytest.raises(PolicyViolationError):
            sh.run(["ok", "Bad-Thing"])
        assert len(fake_core.last_shell.calls) == calls_before

    def test_run_async_raises_synchronously(self, fake_core):
        sh = Shell(policy=ExecutionPolicy(deny=["Bad-*"]), cpp_module=fake_core).start()
        self._stub_names(sh, {"Bad-Thing": ["Bad-Thing"]})
        with pytest.raises(PolicyViolationError):
            sh.run_async("Bad-Thing")

    def test_whatif_transform_reaches_backend(self, fake_core):
        sh = Shell(policy=ExecutionPolicy(dry_run_destructive=True),
                   cpp_module=fake_core).start()
        self._stub_names(sh, {"Remove-Item x": ["Remove-Item"]})
        res = sh.run("Remove-Item x")
        assert res.out == "& { $WhatIfPreference = $true; Remove-Item x }"

    def test_script_content_is_inspected(self, fake_core, tmp_path):
        script = tmp_path / "danger.ps1"
        script.write_text("Remove-Item -Recurse C:\\data")
        sh = Shell(policy=ExecutionPolicy(deny=["Remove-*"]), cpp_module=fake_core).start()
        sh._command_names = lambda text: ("Remove-Item",)  # type: ignore
        with pytest.raises(PolicyViolationError):
            sh.script(script)

    def test_no_policy_means_no_extraction(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        sh.run("anything")  # would explode if extraction ran against the fake
        assert fake_core.last_shell.calls[-1][0] == "execute"

    def test_policy_can_be_swapped_at_runtime(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        self._stub_names(sh, {"Remove-Item x": ["Remove-Item"]})
        assert sh.run("Remove-Item x").success
        sh.policy = ExecutionPolicy(deny=["Remove-*"])
        self._stub_names(sh, {"Remove-Item x": ["Remove-Item"]})
        with pytest.raises(PolicyViolationError):
            sh.run("Remove-Item x")
