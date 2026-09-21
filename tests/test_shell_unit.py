"""Unit tests for the Shell wrapper, run against a fake `_core` module.

The fake (see conftest.FakeCoreModule) records every call the wrapper makes,
so these tests verify configuration wiring and argument translation without
spawning a real PowerShell process.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from virtualshell.errors import (
    ExecutionError,
    ExecutionTimeoutError,
    PowerShellNotFoundError,
    VirtualShellError,
)
from virtualshell.shell import Shell, quote_pwsh_literal

from conftest import FakeExecutionResult

UTF8_COMMAND = "$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()"


class TestConfigWiring:
    def test_defaults(self, fake_core):
        Shell(cpp_module=fake_core)
        cfg = fake_core.last_shell.cfg
        assert cfg.timeout_seconds == 5
        assert cfg.auto_restart_on_timeout is True
        assert cfg.stdin_buffer_size == 64 * 1024
        assert cfg.initial_commands == [UTF8_COMMAND]
        assert cfg.powershell_path == ""  # backend resolves the executable
        assert cfg.powershell_edition == "auto"

    @pytest.mark.parametrize("given,expected", [
        ("auto", "auto"),
        ("core", "core"),
        ("Desktop", "desktop"),
        (" PWSH ", "core"),
        ("powershell", "desktop"),
        ("windows", "desktop"),
        (None, "auto"),
    ])
    def test_edition_is_normalized(self, fake_core, monkeypatch, given, expected):
        monkeypatch.setattr("virtualshell.shell._IS_WINDOWS", True)
        Shell(powershell_edition=given, cpp_module=fake_core)
        assert fake_core.last_shell.cfg.powershell_edition == expected

    def test_invalid_edition_is_rejected(self, fake_core):
        with pytest.raises(ValueError, match="powershell_edition"):
            Shell(powershell_edition="powershell9", cpp_module=fake_core)

    def test_desktop_edition_requires_windows(self, fake_core, monkeypatch):
        monkeypatch.setattr("virtualshell.shell._IS_WINDOWS", False)
        with pytest.raises(ValueError, match="Windows"):
            Shell(powershell_edition="desktop", cpp_module=fake_core)
        # core/auto remain valid everywhere
        Shell(powershell_edition="core", cpp_module=fake_core)
        Shell(powershell_edition="auto", cpp_module=fake_core)

    def test_explicit_path_and_edition_are_both_forwarded(self, fake_core, monkeypatch):
        monkeypatch.setattr("virtualshell.shell._IS_WINDOWS", True)
        sh = Shell(powershell_path="C:/tools/pwsh.exe", powershell_edition="desktop",
                   cpp_module=fake_core)
        cfg = fake_core.last_shell.cfg
        assert cfg.powershell_path == "C:/tools/pwsh.exe"   # the backend lets the path win
        assert cfg.powershell_edition == "desktop"
        assert sh.configured_edition == "desktop"

    def test_explicit_values(self, fake_core, tmp_path):
        Shell(
            powershell_path="C:/tools/pwsh.exe",
            working_directory=tmp_path,
            timeout_seconds=12.7,
            auto_restart_on_timeout=False,
            stdin_buffer_size=1024,
            cpp_module=fake_core,
        )
        cfg = fake_core.last_shell.cfg
        assert cfg.powershell_path == "C:/tools/pwsh.exe"
        assert cfg.working_directory == str(Path(tmp_path).resolve())
        assert cfg.timeout_seconds == 12  # truncated to int
        assert cfg.auto_restart_on_timeout is False
        assert cfg.stdin_buffer_size == 1024

    def test_environment_is_copied(self, fake_core):
        env = {"A": "1"}
        Shell(environment=env, cpp_module=fake_core)
        env["B"] = "2"
        assert fake_core.last_shell.cfg.environment == {"A": "1"}

    def test_initial_commands_stringified_and_utf8_prepended(self, fake_core):
        Shell(initial_commands=[1, "two"], cpp_module=fake_core)
        assert fake_core.last_shell.cfg.initial_commands == [UTF8_COMMAND, "1", "two"]

    def test_set_utf8_false_skips_prelude(self, fake_core):
        Shell(initial_commands=["x"], set_UTF8=False, cpp_module=fake_core)
        assert fake_core.last_shell.cfg.initial_commands == ["x"]

    def test_session_paths_wired_into_config(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        cfg = fake_core.last_shell.cfg
        assert cfg.session_snapshot_path == str(sh.session_path)
        assert cfg.restore_script_path.endswith("get-session.ps1")
        assert sh.session_path.name == f"session_{sh.python_run_id}.xml"

    def test_run_ids_are_unique(self, fake_core):
        a = Shell(cpp_module=fake_core)
        b = Shell(cpp_module=fake_core)
        assert a.python_run_id != b.python_run_id
        assert a.session_path != b.session_path


class TestLifecycle:
    def test_start_success_sets_pid_and_chains(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        assert sh.start() is sh
        assert sh.pid == 4242
        assert sh.is_running is True

    def test_start_when_already_alive_is_noop(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        calls_before = list(fake_core.last_shell.calls)
        assert sh.start() is sh
        assert fake_core.last_shell.calls == calls_before

    def test_start_failure_raises(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        fake_core.last_shell.start_result = False
        with pytest.raises(PowerShellNotFoundError):
            sh.start()

    def test_stop_forwards_force_flag(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        sh.stop(force=True)
        assert ("stop", True) in fake_core.last_shell.calls
        assert sh.is_running is False

    def test_stop_removes_session_file(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        sh.session_path.write_text("snapshot")
        sh.stop()
        assert not sh.session_path.exists()

    def test_stop_wraps_backend_errors(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        fake_core.last_shell.raise_on_stop = RuntimeError("kaboom")
        with pytest.raises(VirtualShellError, match="kaboom"):
            sh.stop()

    def test_context_manager_starts_and_stops(self, fake_core):
        with Shell(cpp_module=fake_core) as sh:
            assert sh.is_running is True
        assert sh.is_running is False

    def test_wait_if_restarting_blocks_until_ready(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.restart_polls_remaining = 2
        res = sh.run("Get-Date")  # must poll through the restart before executing
        assert res.success
        assert fake_core.last_shell.restart_polls_remaining == 0


class TestRun:
    def test_single_command_passes_string_and_timeout(self, fake_core):
        sh = Shell(timeout_seconds=9, cpp_module=fake_core).start()
        res = sh.run("Get-Date")
        assert res.out == "Get-Date"
        assert ("execute", "Get-Date", 9.0) in fake_core.last_shell.calls

    def test_explicit_timeout_overrides_default(self, fake_core):
        sh = Shell(timeout_seconds=9, cpp_module=fake_core).start()
        sh.run("x", timeout=2.5)
        assert ("execute", "x", 2.5) in fake_core.last_shell.calls

    def test_batch_returns_list_in_order(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        results = sh.run(["a", "b", "c"])
        assert [r.out for r in results] == ["a", "b", "c"]
        assert ("execute_batch", ["a", "b", "c"], 5.0) in fake_core.last_shell.calls

    def test_strip_results_default_on(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out=f"  {cmd}  ")
        assert sh.run("hi").out == "hi"
        assert [r.out for r in sh.run(["a", "b"])] == ["a", "b"]

    def test_set_strip_results_toggle(self, fake_core):
        sh = Shell(strip_results=False, cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out=f" {cmd} ")
        assert sh.run("x").out == " x "
        sh.set_strip_results(True)
        assert sh.run("x").out == "x"

    def test_raise_on_error_single(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(
            err="boom", exit_code=1, success=False)
        assert sh.run("bad").success is False  # default: no raise
        with pytest.raises(ExecutionError, match="boom"):
            sh.run("bad", raise_on_error=True)

    def test_raise_on_error_batch(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: (
            FakeExecutionResult(out=cmd) if cmd != "bad"
            else FakeExecutionResult(err="boom", exit_code=1, success=False))
        with pytest.raises(ExecutionError, match="boom"):
            sh.run(["ok", "bad"], raise_on_error=True)

    def test_timeout_raises_when_auto_restart_disabled(self, fake_core):
        sh = Shell(auto_restart_on_timeout=False, cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(
            err="Command timed out", exit_code=-1, success=False)
        with pytest.raises(ExecutionTimeoutError):
            sh.run("Start-Sleep 99")

    def test_timeout_does_not_raise_when_auto_restart_enabled(self, fake_core):
        sh = Shell(auto_restart_on_timeout=True, cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(
            err="Command timed out", exit_code=-1, success=False)
        res = sh.run("Start-Sleep 99")
        assert res.exit_code == -1

    def test_pwsh_quotes_literal(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        res = sh.pwsh("it's")
        assert res.out == quote_pwsh_literal("it's") == "'it''s'"


class TestRunAsync:
    def test_single_returns_future_and_calls_callback(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        seen = []
        fut = sh.run_async("Get-Date", callback=seen.append)
        assert fut.result(timeout=1).out == "Get-Date"
        assert len(seen) == 1 and seen[0].out == "Get-Date"

    def test_callback_exceptions_are_suppressed(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()

        def bad_callback(_):
            raise RuntimeError("callback bug")

        fut = sh.run_async("x", callback=bad_callback)
        assert fut.result(timeout=1).success

    def test_batch_forwards_options_and_reports_progress(self, fake_core):
        sh = Shell(timeout_seconds=3, cpp_module=fake_core).start()
        progress = []
        fut = sh.run_async(["a", "b"], callback=progress.append)
        results = fut.result(timeout=1)
        assert [r.out for r in results] == ["a", "b"]
        assert ("execute_async_batch", ["a", "b"], True, 3.0) in fake_core.last_shell.calls
        assert [p.currentCommand for p in progress] == [1, 2]
        assert progress[-1].isComplete is True


class TestScript:
    def test_positional_args(self, fake_core, tmp_path):
        script = tmp_path / "s.ps1"
        script.write_text("param($a) $a")
        sh = Shell(cpp_module=fake_core).start()
        res = sh.script(script, ["one", "two"])
        assert res.success
        name, path, args, timeout, dot_source, raise_on_error = fake_core.last_shell.calls[-1]
        assert name == "execute_script"
        assert path == str(script.resolve())
        assert args == ["one", "two"]
        assert timeout == 5.0
        assert dot_source is False
        assert raise_on_error is False  # backend must never double-throw

    def test_named_args(self, fake_core, tmp_path):
        script = tmp_path / "s.ps1"
        script.write_text("param($x) $x")
        sh = Shell(cpp_module=fake_core).start()
        sh.script(script, {"x": "1"}, dot_source=True)
        name, path, named, timeout, dot_source, raise_on_error = fake_core.last_shell.calls[-1]
        assert name == "execute_script_kv"
        assert named == {"x": "1"}
        assert dot_source is True
        assert raise_on_error is False

    def test_raise_on_error_is_python_side(self, fake_core, tmp_path):
        script = tmp_path / "s.ps1"
        script.write_text("boom")
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.script_result_factory = lambda: FakeExecutionResult(
            err="script failed", exit_code=1, success=False)
        assert sh.script(script).success is False
        with pytest.raises(ExecutionError, match="script failed"):
            sh.script(script, raise_on_error=True)

    def test_script_async_callback(self, fake_core, tmp_path):
        script = tmp_path / "s.ps1"
        script.write_text("1")
        sh = Shell(cpp_module=fake_core).start()
        seen = []
        fut = sh.script_async(script, ["a"], callback=seen.append)
        assert fut.result(timeout=1).success
        assert len(seen) == 1

    def test_script_async_kv_callback_via_future(self, fake_core, tmp_path):
        script = tmp_path / "s.ps1"
        script.write_text("1")
        sh = Shell(cpp_module=fake_core).start()
        seen = []
        fut = sh.script_async(script, {"x": "1"}, callback=seen.append)
        assert fut.result(timeout=1).success
        assert len(seen) == 1


class TestConvenience:
    def test_save_session_invokes_bundled_script(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        res = sh.save_session()
        assert res.success
        name, path, args, *_ = fake_core.last_shell.calls[-1]
        assert name == "execute_script"
        assert path.endswith("save-session.ps1")
        assert args == [str(sh.session_path)]

    def test_make_proxy_constructs_python_proxy(self, fake_core, monkeypatch):
        import virtualshell.ps_proxy as ps_proxy_module

        created = {}

        class StubProxy:
            def __init__(self, shell, type_name, object_ref, *, static=False):
                created["args"] = (shell, type_name, object_ref, static)

        monkeypatch.setattr(ps_proxy_module, "PsProxy", StubProxy)
        sh = Shell(cpp_module=fake_core).start()
        proxy = sh.make_proxy("System.IO.FileInfo", "$file")
        assert isinstance(proxy, StubProxy)
        assert created["args"] == (sh, "System.IO.FileInfo", "$file", False)

        sh.make_proxy("System.Math", "$t", static=True)
        assert created["args"] == (sh, "System.Math", "$t", True)

    def test_make_proxy_accepts_generated_protocol_class(self, fake_core, monkeypatch):
        import virtualshell.ps_proxy as ps_proxy_module

        created = {}

        class StubProxy:
            def __init__(self, shell, type_name, object_ref, *, static=False):
                created["args"] = (type_name, object_ref, static)

        class StringBuilder:  # shaped like generate_psobject output
            __ps_type_name__ = "System.Text.StringBuilder"
            __ps_expression__ = "[System.Text.StringBuilder]::new()"

        monkeypatch.setattr(ps_proxy_module, "PsProxy", StubProxy)
        sh = Shell(cpp_module=fake_core).start()

        # Default: recreate the object from the embedded expression.
        sh.make_proxy(StringBuilder)
        assert created["args"] == ("System.Text.StringBuilder",
                                   "[System.Text.StringBuilder]::new()", False)

        # Explicit obj_ref binds an existing variable instead.
        sh.make_proxy(StringBuilder, "$existing")
        assert created["args"] == ("System.Text.StringBuilder", "$existing", False)

    def test_make_proxy_honours_static_protocol_metadata(self, fake_core, monkeypatch):
        import virtualshell.ps_proxy as ps_proxy_module

        created = {}

        class StubProxy:
            def __init__(self, shell, type_name, object_ref, *, static=False):
                created["args"] = (type_name, object_ref, static)

        class Math:  # shaped like generate_psobject output for '[System.Math]'
            __ps_type_name__ = "System.Math"
            __ps_expression__ = "[System.Math]"
            __ps_static__ = True

        monkeypatch.setattr(ps_proxy_module, "PsProxy", StubProxy)
        sh = Shell(cpp_module=fake_core).start()

        sh.make_proxy(Math)
        assert created["args"] == ("System.Math", "[System.Math]", True)

        # Static metadata sticks when binding an existing type variable.
        sh.make_proxy(Math, "$existing_type")
        assert created["args"] == ("System.Math", "$existing_type", True)

    def test_make_proxy_rejects_class_without_metadata(self, fake_core):
        class Naked:
            pass

        sh = Shell(cpp_module=fake_core).start()
        with pytest.raises(TypeError, match="metadata"):
            sh.make_proxy(Naked)

    def test_repr_reflects_running_state(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        assert repr(sh) == "<Shell running=0>"
        sh.start()
        assert repr(sh) == "<Shell running=1>"


class TestEditionDetection:
    def test_edition_property_starts_backend_and_caches(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        fake = fake_core.last_shell
        fake.edition_result = "desktop"
        assert sh.edition == "desktop"
        assert fake.alive                       # auto-started, like make_proxy()
        assert sh.edition == "desktop"
        assert fake.calls.count(("get_powershell_edition",)) == 1

    def test_stop_clears_cached_edition_and_version(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        fake = fake_core.last_shell
        assert sh.edition == "core"
        assert sh.powershell_version == "7.6.6"
        sh.stop()
        fake.edition_result = "desktop"
        fake.version_result = "5.1.22621.4391"
        assert sh.edition == "desktop"
        assert sh.powershell_version == "5.1.22621.4391"

    def test_unrecognised_edition_raises(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        fake_core.last_shell.edition_result = ""
        with pytest.raises(VirtualShellError):
            _ = sh.edition

    def test_powershell_path_reports_resolved_executable(self, fake_core):
        sh = Shell(cpp_module=fake_core)
        assert sh.powershell_path == ""          # nothing resolved before start()
        sh.start()
        assert sh.powershell_path == "C:/fake/pwsh.exe"
        sh.stop()
        assert sh.powershell_path == ""

    def test_powershell_path_falls_back_to_configured_path(self, fake_core):
        sh = Shell(powershell_path="C:/tools/pwsh.exe", cpp_module=fake_core)
        assert sh.powershell_path == "C:/tools/pwsh.exe"

    def test_start_failure_message_names_edition(self, fake_core, monkeypatch):
        monkeypatch.setattr("virtualshell.shell._IS_WINDOWS", True)
        sh = Shell(powershell_edition="desktop", cpp_module=fake_core)
        fake_core.last_shell.start_result = False
        with pytest.raises(PowerShellNotFoundError, match="Windows PowerShell 5.1"):
            sh.start()
