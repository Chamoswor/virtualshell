"""Integration tests for the agent-facing surface against a real PowerShell:
run_objects, output budgets, ExecutionPolicy enforcement, prompt blocking,
checkpoints/restore, interrupt and tool-schema generation.

Same requirements and skip behavior as test_integration.py.
"""
from __future__ import annotations

import time

import pytest

from conftest import integration

pytestmark = integration


@pytest.fixture(scope="module")
def shell(edition):
    from virtualshell import Shell

    sh = Shell(timeout_seconds=30, powershell_edition=edition).start()
    yield sh
    sh.stop(force=True)


class TestAutoStart:
    def test_run_without_start(self, edition):
        from virtualshell import Shell

        sh = Shell(timeout_seconds=30, powershell_edition=edition)
        try:
            assert sh.run("1+1").out == "2"  # strip default + auto-start
        finally:
            sh.stop(force=True)


class TestRunObjects:
    def test_select_returns_dicts(self, shell):
        objs = shell.run_objects("Get-Process -Id $PID", select=["Name", "Id"])
        assert len(objs) == 1
        assert set(objs[0]) == {"Name", "Id"}
        assert isinstance(objs[0]["Id"], int)

    def test_single_scalar_still_a_list(self, shell):
        assert shell.run_objects("42") == [42]

    def test_first_limits(self, shell):
        assert shell.run_objects("1..100", first=3) == [1, 2, 3]

    def test_empty_pipeline_is_empty_list(self, shell):
        assert shell.run_objects(
            "Get-ChildItem Env: | Where-Object Name -eq 'VS_NOPE_X'") == []

    def test_depth_serializes_nesting(self, shell):
        objs = shell.run_objects("@{a=@{b=@{c=1}}}", depth=3)
        assert objs[0]["a"]["b"]["c"] == 1

    def test_assignments_persist_in_session(self, shell):
        shell.run_objects("$vs_obj_state = 'kept'; 'x'")
        assert shell.run("$vs_obj_state").out == "kept"

    def test_terminating_error_raises(self, shell):
        from virtualshell import ExecutionError

        with pytest.raises(ExecutionError, match="boom"):
            shell.run_objects("throw 'boom'")

    def test_stderr_raises_by_default_but_can_be_tolerated(self, shell):
        from virtualshell import ExecutionError

        with pytest.raises(ExecutionError, match="soft"):
            shell.run_objects("Write-Error 'soft'; 1")
        assert shell.run_objects("Write-Error 'soft'; 1",
                                 raise_on_error=False) == [1]

    def test_unicode_survives_json(self, shell):
        assert shell.run_objects("'æøå'") == ["æøå"]


class TestOutputBudget:
    def test_truncation_and_paging(self, shell):
        res = shell.run('1..2000 | ForEach-Object { "line $_" }', max_output=1500)
        assert len(res.out) <= 1500
        assert "fetch_output(" in res.out
        key = res.out.split("fetch_output('")[1].split("'")[0]

        collected, offset = "", 0
        while True:
            page = shell.fetch_output(key, offset=offset, max_output=10000)
            collected += page.text
            if page.next_offset is None:
                break
            offset = page.next_offset
        assert collected.startswith("line 1")
        assert collected.rstrip().endswith("line 2000")
        assert page.total_lines == 2000


class TestPromptBlocking:
    def test_read_host_raises_prompt_blocked(self, shell):
        from virtualshell import PromptBlockedError

        with pytest.raises(PromptBlockedError, match="parameter"):
            shell.run("Read-Host 'value'", raise_on_error=True)

    def test_get_credential_raises_prompt_blocked(self, shell):
        from virtualshell import PromptBlockedError

        with pytest.raises(PromptBlockedError):
            shell.run("Get-Credential", raise_on_error=True)

    def test_without_raise_flag_result_reports_failure(self, shell):
        res = shell.run("Read-Host 'value'")
        assert not res.success
        assert res.err


class TestPolicyLive:
    """End-to-end policy enforcement with the real parser round trip."""

    def test_read_only_lane(self, shell):
        from virtualshell import ExecutionPolicy, PolicyViolationError

        shell.policy = ExecutionPolicy(read_only=True)
        try:
            assert shell.run("Get-Date").success
            with pytest.raises(PolicyViolationError, match="Remove-Item"):
                shell.run(r"Remove-Item C:\definitely\nope.txt")
            # Aliases resolve before matching.
            with pytest.raises(PolicyViolationError, match="Remove-Item"):
                shell.run(r"rm C:\definitely\nope.txt")
            # Nested script-block bodies are seen too.
            with pytest.raises(PolicyViolationError, match="Stop-Process"):
                shell.run("1..3 | ForEach-Object { Stop-Process -Id $_ }")
            # Dynamic invocation cannot be inspected -> blocked.
            with pytest.raises(PolicyViolationError, match="dynamic"):
                shell.run("& $whatever")
            # Pure expressions carry no commands.
            assert shell.run("$vs_pol_x = 5; $vs_pol_x").out == "5"
        finally:
            shell.policy = None

    def test_confirm_flow(self, shell):
        from virtualshell import ExecutionPolicy, PolicyViolationError

        seen = []
        shell.policy = ExecutionPolicy(
            confirm=["Remove-*"], on_confirm=lambda req: seen.append(req) or True)
        try:
            tmp = shell.run("($f = New-TemporaryFile).FullName").out
            assert shell.run(f"Remove-Item '{tmp}'").success
            assert seen and seen[0].matched == ("Remove-Item",)

            shell.policy = ExecutionPolicy(confirm=["Remove-*"],
                                           on_confirm=lambda req: False)
            with pytest.raises(PolicyViolationError, match="denied"):
                shell.run("Remove-Item 'whatever.txt'")
        finally:
            shell.policy = None

    def test_dry_run_destructive(self, shell):
        from virtualshell import ExecutionPolicy

        tmp = None
        shell.policy = ExecutionPolicy(dry_run_destructive=True)
        try:
            tmp = shell.run("($f = New-TemporaryFile).FullName").out
            res = shell.run(f"Remove-Item '{tmp}'")
            assert "What if" in res.out
            assert shell.run(f"Test-Path '{tmp}'").out == "True"
        finally:
            shell.policy = None
            if tmp:
                shell.run(f"Remove-Item '{tmp}' -ErrorAction SilentlyContinue")

    def test_policy_applies_to_scripts(self, shell, tmp_path):
        from virtualshell import ExecutionPolicy, PolicyViolationError

        danger = tmp_path / "danger.ps1"
        danger.write_text("Remove-Item -Recurse $args[0]\n")
        shell.policy = ExecutionPolicy(read_only=True)
        try:
            with pytest.raises(PolicyViolationError, match="Remove-Item"):
                shell.script(danger, ["C:\\nope"])
        finally:
            shell.policy = None


class TestCheckpointRestore:
    def test_named_checkpoint_roundtrip(self, shell):
        shell.run("$vs_cp = 'original'")
        cp = shell.checkpoint("agent-step-1")
        assert cp.name == "agent-step-1"
        assert cp.path.exists()
        assert any(c.name == "agent-step-1" for c in shell.checkpoints)

        shell.run("$vs_cp = 'changed'")
        shell.restore("agent-step-1")
        assert shell.run("$vs_cp").out == "original"

    def test_restore_default_uses_newest(self, shell):
        shell.run("$vs_cp2 = 'v1'")
        shell.checkpoint("newest")
        shell.run("$vs_cp2 = 'v2'")
        shell.restore()
        assert shell.run("$vs_cp2").out == "v1"

    def test_unknown_name_lists_available(self, shell):
        from virtualshell import VirtualShellError

        with pytest.raises(VirtualShellError, match="unknown checkpoint"):
            shell.restore("no-such-checkpoint")


class TestInterrupt:
    def test_interrupt_cancels_and_restores(self, edition):
        from virtualshell import Shell

        sh = Shell(timeout_seconds=60, powershell_edition=edition).start()
        try:
            sh.run("$global:vs_int = 'kept'")
            sh.checkpoint("pre")

            t0 = time.time()
            fut = sh.run_async("Start-Sleep -Seconds 30; 'DONE'")
            time.sleep(0.5)
            assert sh.interrupt() is True
            res = fut.result(timeout=10)
            elapsed = time.time() - t0

            assert not res.success          # the command was aborted...
            assert elapsed < 15             # ...long before its 30s sleep
            assert sh.is_running
            assert sh.run("$global:vs_int").out == "kept"  # state came back
        finally:
            sh.stop(force=True)

    def test_interrupt_when_idle_still_restarts(self, shell):
        shell.run("$vs_idle = 1")
        shell.checkpoint("idle")
        assert shell.interrupt() is True
        assert shell.is_running
        assert shell.run("$vs_idle").out == "1"


class TestSchemas:
    def test_command_schema_shape(self, shell):
        schema = shell.command_schema("Get-Process")
        assert schema["name"] == "Get-Process"
        assert schema["description"]
        props = schema["inputSchema"]["properties"]
        assert props["Id"]["type"] == "array"
        assert props["Id"]["items"]["type"] == "integer"
        assert props["Id"]["x-ps-type"] == "System.Int32[]"
        # Common parameters are excluded.
        assert "ErrorAction" not in props
        assert "Verbose" not in props

    def test_alias_resolves_to_target(self, shell):
        schema = shell.command_schema("gci", include_help=False)
        assert schema["name"] == "Get-ChildItem"

    def test_validate_set_becomes_enum(self, shell):
        # Get-ChildItem -Attributes is an enum-typed parameter on both editions;
        # use Out-File's -Encoding ValidateSet on desktop, enum on core. Use a
        # function we define ourselves for a deterministic check instead.
        shell.run(
            "function global:Test-VsSchema { param("
            "[ValidateSet('alpha','beta')] [string]$Mode, "
            "[Parameter(Mandatory=$true)] [int]$Count, "
            "[switch]$Force) }")
        schema = shell.command_schema("Test-VsSchema", include_help=False)
        props = schema["inputSchema"]["properties"]
        assert props["Mode"]["enum"] == ["alpha", "beta"]
        assert props["Count"]["type"] == "integer"
        assert props["Force"]["type"] == "boolean"
        assert schema["inputSchema"]["required"] == ["Count"]

    def test_missing_command_raises(self, shell):
        from virtualshell import ExecutionError

        with pytest.raises(ExecutionError):
            shell.command_schema("Get-DefinitelyNotAThing")

    def test_module_schemas(self, shell):
        schemas = shell.module_schemas("Microsoft.PowerShell.Management",
                                       include_help=False, timeout=120)
        names = {s["name"] for s in schemas}
        assert "Get-ChildItem" in names
        assert all("inputSchema" in s for s in schemas)
