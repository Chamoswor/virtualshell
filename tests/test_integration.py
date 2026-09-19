"""Integration tests: the full stack against a real PowerShell process.

These require the compiled `virtualshell._core` extension (i.e. the package
was built/installed) and a `pwsh` executable on PATH. They skip themselves
otherwise, so the unit tests still run on machines without a toolchain.
"""
from __future__ import annotations

import shutil
import time

import pytest


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

    sh = Shell(timeout_seconds=30).start()
    yield sh
    sh.stop(force=True)


class TestBasicExecution:
    def test_echo(self, shell):
        res = shell.run("Write-Output 'hello'")
        assert res.success
        assert res.exit_code == 0
        assert res.out.strip() == "hello"
        assert res.execution_time >= 0.0

    def test_unicode_round_trip(self, shell):
        res = shell.run("Write-Output 'æøå ÆØÅ'")
        assert res.success
        assert res.out.strip() == "æøå ÆØÅ"

    def test_unicode_command_arrives_intact(self, shell):
        # If stdin were decoded with the OEM code page, 'æøå' would arrive as
        # six mangled characters server-side and .Length would report 6.
        res = shell.run("'æøå'.Length")
        assert res.out.strip() == "3"

    def test_pwsh_literal_with_quotes(self, shell):
        res = shell.pwsh("it's a 'test'")
        assert res.success
        assert res.out.strip() == "it's a 'test'"

    def test_state_persists_between_commands(self, shell):
        assert shell.run("$vs_test_state = 41").success
        res = shell.run("$vs_test_state + 1")
        assert res.out.strip() == "42"

    def test_batch_preserves_order(self, shell):
        results = shell.run(["Write-Output 'one'", "Write-Output 'two'", "Write-Output 'three'"])
        assert [r.out.strip() for r in results] == ["one", "two", "three"]
        assert all(r.success for r in results)

    def test_failing_command_reports_error(self, shell):
        res = shell.run("Get-Item 'C:/definitely/not/a/real/path.xyz'")
        assert not res.success
        assert res.err  # PowerShell error text captured on stderr

    def test_raise_on_error(self, shell):
        from virtualshell import ExecutionError

        with pytest.raises(ExecutionError):
            shell.run("Get-Item 'C:/definitely/not/a/real/path.xyz'", raise_on_error=True)


class TestAsyncExecution:
    def test_run_async_future(self, shell):
        fut = shell.run_async("Write-Output 'async'")
        res = fut.result(timeout=30)
        assert res.success
        assert res.out.strip() == "async"

    def test_run_async_callback(self, shell):
        seen = []
        fut = shell.run_async("Write-Output 'cb'", callback=seen.append)
        fut.result(timeout=30)
        deadline = time.time() + 5
        while not seen and time.time() < deadline:
            time.sleep(0.05)
        assert seen and seen[0].out.strip() == "cb"

    def test_batch_async_with_progress(self, shell):
        progress = []
        fut = shell.run_async(["Write-Output 'a'", "Write-Output 'b'"], callback=progress.append)
        results = fut.result(timeout=60)
        assert [r.out.strip() for r in results] == ["a", "b"]
        assert progress and progress[-1].isComplete


class TestScripts:
    def test_positional_args(self, shell, tmp_path):
        script = tmp_path / "positional.ps1"
        script.write_text('param($a, $b)\nWrite-Output "$a-$b"\n', encoding="utf-8")
        res = shell.script(script, ["left", "right"], raise_on_error=True)
        assert res.out.strip() == "left-right"

    def test_named_args(self, shell, tmp_path):
        script = tmp_path / "named.ps1"
        script.write_text('param($Name, $Count)\nWrite-Output ("{0}x{1}" -f $Name, $Count)\n', encoding="utf-8")
        res = shell.script(script, {"Name": "vs", "Count": "3"}, raise_on_error=True)
        assert res.out.strip() == "vsx3"

    def test_dot_source_persists_state(self, shell, tmp_path):
        script = tmp_path / "dotsource.ps1"
        script.write_text("$vs_dot_sourced = 'kept'\n", encoding="utf-8")
        shell.script(script, dot_source=True, raise_on_error=True)
        res = shell.run("$vs_dot_sourced")
        assert res.out.strip() == "kept"


class TestTimeoutAndRestart:
    def test_timeout_reports_exit_code_and_recovers(self, shell):
        res = shell.run("Start-Sleep -Seconds 10", timeout=1)
        assert not res.success
        assert res.exit_code == -1
        # auto_restart_on_timeout=True: the backend restarts and accepts new work.
        deadline = time.time() + 30
        while time.time() < deadline:
            follow_up = shell.run("40 + 2")
            if follow_up.success and follow_up.out.strip() == "42":
                break
            time.sleep(0.2)
        else:
            pytest.fail("shell did not recover after timeout-triggered restart")


class TestSeparateInstances:
    def test_environment_variables(self):
        from virtualshell import Shell

        with Shell(timeout_seconds=30, environment={"VS_TEST_ENV": "hello-env"}) as sh:
            res = sh.run("$env:VS_TEST_ENV")
            assert res.out.strip() == "hello-env"

    def test_working_directory(self, tmp_path):
        from virtualshell import Shell

        with Shell(timeout_seconds=30, working_directory=tmp_path) as sh:
            res = sh.run("(Get-Location).Path")
            assert res.out.strip().rstrip("\\/") == str(tmp_path.resolve()).rstrip("\\/")

    def test_strip_results(self):
        from virtualshell import Shell

        with Shell(timeout_seconds=30, strip_results=True) as sh:
            res = sh.run("Write-Output 'clean'")
            assert res.out == "clean"

    def test_stop_removes_session_snapshot(self):
        from virtualshell import Shell

        sh = Shell(timeout_seconds=30).start()
        session_path = sh.session_path
        sh.save_session(timeout=60)
        assert session_path.exists()
        sh.stop()
        assert not session_path.exists()
