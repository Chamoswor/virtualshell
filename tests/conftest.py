"""Shared fixtures for the virtualshell test suite.

The unit tests exercise the Python wrapper (`virtualshell.shell.Shell`)
against an in-memory fake of the compiled `_core` extension, injected via the
``cpp_module`` constructor parameter. They therefore run on any platform,
even when the extension has not been built: in that case a minimal stub is
registered as ``virtualshell._core`` so that importing ``virtualshell.shell``
succeeds.

Integration tests (see test_integration.py) require the real extension and a
PowerShell host, and skip themselves otherwise. They are parametrized over
every edition found on the machine via the ``edition`` fixture: ``"core"``
(pwsh on PATH) and ``"desktop"`` (Windows PowerShell 5.1, Windows only).
"""
from __future__ import annotations

import os
import shutil
import sys
import types
from concurrent.futures import Future
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest


def _install_core_stub_if_missing() -> None:
    try:
        import virtualshell._core  # noqa: F401
    except ImportError:
        stub = types.ModuleType("virtualshell._core")
        stub.__vs_stub__ = True

        class _Placeholder:
            pass

        stub.ExecutionResult = _Placeholder
        stub.BatchProgress = _Placeholder
        stub.Config = _Placeholder
        stub.VirtualShell = _Placeholder
        sys.modules["virtualshell._core"] = stub


_install_core_stub_if_missing()


# ---------- Integration test discovery ----------

def real_core_available() -> bool:
    try:
        import virtualshell._core as core
    except ImportError:
        return False
    return not getattr(core, "__vs_stub__", False)


def _windows_powershell_available() -> bool:
    if os.name != "nt":
        return False
    root = os.environ.get("SystemRoot", r"C:\Windows")
    exe = Path(root) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    return exe.is_file() or shutil.which("powershell") is not None


def available_editions() -> List[str]:
    """PowerShell editions the integration tests can run against here."""
    if not real_core_available():
        return []
    editions: List[str] = []
    if shutil.which("pwsh"):
        editions.append("core")
    if _windows_powershell_available():
        editions.append("desktop")
    return editions


EDITIONS = available_editions()

integration = pytest.mark.skipif(
    not EDITIONS,
    reason="requires the compiled _core extension and pwsh or Windows PowerShell 5.1",
)


@pytest.fixture(scope="module", params=EDITIONS)
def edition(request) -> str:
    """Edition under test: 'core' (pwsh) or 'desktop' (Windows PowerShell 5.1)."""
    return request.param


# ---------- Fake C++ surface for unit tests ----------

class FakeExecutionResult:
    def __init__(
        self,
        out: Any = "",
        err: Any = "",
        exit_code: int = 0,
        success: bool = True,
        execution_time: float = 0.0,
    ) -> None:
        self.out = out
        self.err = err
        self.exit_code = exit_code
        self.success = success
        self.execution_time = execution_time


class FakeBatchProgress:
    def __init__(
        self,
        currentCommand: int,
        totalCommands: int,
        lastResult: FakeExecutionResult,
        isComplete: bool,
        allResults: List[FakeExecutionResult],
    ) -> None:
        self.currentCommand = currentCommand
        self.totalCommands = totalCommands
        self.lastResult = lastResult
        self.isComplete = isComplete
        self.allResults = allResults


class FakeConfig:
    def __init__(self) -> None:
        self.powershell_path = ""
        self.powershell_edition = "auto"
        self.working_directory = ""
        self.capture_output = True
        self.capture_error = True
        self.auto_restart_on_timeout = True
        self.timeout_seconds = 0
        self.environment: Dict[str, str] = {}
        self.initial_commands: List[str] = []
        self.restore_script_path = ""
        self.session_snapshot_path = ""
        self.stdin_buffer_size = 0


def _echo_result(command: str) -> FakeExecutionResult:
    return FakeExecutionResult(out=command, exit_code=0, success=True)


class FakeVirtualShell:
    """Records every call made by the Shell wrapper and returns canned results.

    ``result_factory`` maps a command string to the FakeExecutionResult the
    fake backend returns; the default echoes the command back in ``out`` so
    tests can assert on exactly what the wrapper sent down.
    """

    def __init__(self, cfg: FakeConfig) -> None:
        self.cfg = cfg
        self.calls: List[tuple] = []
        self.alive = False
        self.start_result = True
        self.raise_on_stop: Optional[Exception] = None
        self.restart_polls_remaining = 0
        self.result_factory: Callable[[str], FakeExecutionResult] = _echo_result
        self.script_result_factory: Callable[[], FakeExecutionResult] = FakeExecutionResult
        self.edition_result = "core"
        self.version_result = "7.6.6"
        self.resolved_path = "C:/fake/pwsh.exe"

    # -- process control --
    def start(self) -> bool:
        self.calls.append(("start",))
        if self.start_result:
            self.alive = True
        return self.start_result

    def stop(self, force: bool = False) -> None:
        self.calls.append(("stop", force))
        if self.raise_on_stop is not None:
            raise self.raise_on_stop
        self.alive = False

    def is_alive(self) -> bool:
        return self.alive

    def is_restarting(self) -> bool:
        if self.restart_polls_remaining > 0:
            self.restart_polls_remaining -= 1
            return True
        return False

    def get_process_id(self) -> int:
        return 4242

    def get_powershell_edition(self) -> str:
        self.calls.append(("get_powershell_edition",))
        return self.edition_result

    def get_powershell_version(self) -> str:
        self.calls.append(("get_powershell_version",))
        return self.version_result

    def get_resolved_powershell_path(self) -> str:
        return self.resolved_path if self.alive else ""

    # -- sync execution --
    def execute(self, command: str, timeout_seconds: float = 0.0) -> FakeExecutionResult:
        self.calls.append(("execute", command, timeout_seconds))
        return self.result_factory(command)

    def execute_batch(self, commands: List[str], timeout_seconds: float = 0.0) -> List[FakeExecutionResult]:
        self.calls.append(("execute_batch", list(commands), timeout_seconds))
        return [self.result_factory(c) for c in commands]

    def execute_script(self, script_path: str, args: List[str], timeout_seconds: float,
                       dot_source: bool, raise_on_error: bool) -> FakeExecutionResult:
        self.calls.append(("execute_script", script_path, list(args), timeout_seconds, dot_source, raise_on_error))
        return self.script_result_factory()

    def execute_script_kv(self, script_path: str, named_args: Dict[str, str], timeout_seconds: float,
                          dot_source: bool, raise_on_error: bool) -> FakeExecutionResult:
        self.calls.append(("execute_script_kv", script_path, dict(named_args), timeout_seconds, dot_source, raise_on_error))
        return self.script_result_factory()

    # -- async execution --
    def execute_async(self, command: str, callback=None, timeout_seconds: float = 0.0) -> Future:
        self.calls.append(("execute_async", command, timeout_seconds))
        res = self.result_factory(command)
        if callback is not None:
            callback(res)
        fut: Future = Future()
        fut.set_result(res)
        return fut

    def execute_async_batch(self, commands: List[str], progress_callback=None,
                            stop_on_first_error: bool = True,
                            per_command_timeout_seconds: float = 0.0) -> Future:
        self.calls.append(("execute_async_batch", list(commands), stop_on_first_error, per_command_timeout_seconds))
        results: List[FakeExecutionResult] = []
        for i, c in enumerate(commands):
            res = self.result_factory(c)
            results.append(res)
            if progress_callback is not None:
                progress_callback(FakeBatchProgress(
                    currentCommand=i + 1,
                    totalCommands=len(commands),
                    lastResult=res,
                    isComplete=(i + 1 == len(commands)),
                    allResults=list(results),
                ))
        fut: Future = Future()
        fut.set_result(results)
        return fut

    def execute_async_script(self, script_path: str, args: List[str], callback=None,
                             timeout_seconds: float = 0.0, dot_source: bool = False,
                             raise_on_error: bool = False) -> Future:
        self.calls.append(("execute_async_script", script_path, list(args), timeout_seconds, dot_source, raise_on_error))
        res = self.script_result_factory()
        if callback is not None:
            callback(res)
        fut: Future = Future()
        fut.set_result(res)
        return fut

    def execute_async_script_kv(self, script_path: str, named_args: Dict[str, str],
                                timeout_seconds: float = 0.0, dot_source: bool = False,
                                raise_on_error: bool = False) -> Future:
        self.calls.append(("execute_async_script_kv", script_path, dict(named_args), timeout_seconds, dot_source, raise_on_error))
        fut: Future = Future()
        fut.set_result(self.script_result_factory())
        return fut

    # -- misc --
    def make_proxy(self, type_name: str, object_ref: str = "$obj", depth: int = 4):
        self.calls.append(("make_proxy", type_name, object_ref, depth))
        return object()


class FakeCoreModule:
    """Stands in for the compiled `virtualshell._core` module."""

    def __init__(self) -> None:
        self.Config = FakeConfig
        self.ExecutionResult = FakeExecutionResult
        self.BatchProgress = FakeBatchProgress
        self.last_shell: Optional[FakeVirtualShell] = None

        def _factory(cfg: FakeConfig) -> FakeVirtualShell:
            self.last_shell = FakeVirtualShell(cfg)
            return self.last_shell

        self.VirtualShell = _factory


@pytest.fixture
def fake_core() -> FakeCoreModule:
    return FakeCoreModule()
