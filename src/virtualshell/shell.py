"""
High-level: Python façade over a C++ PowerShell runner (virtualshell_core).

Design goals (production-readiness):
- **Thin wrapper**: All heavy I/O and process orchestration live in C++ for performance.
- **No surprises**: Stable API; no implicit state mutations beyond what is documented.
- **Clear failure modes**: Dedicated exceptions and `raise_on_error` semantics.
- **Thread-friendly**: Async methods return Futures and accept callbacks; no Python-side locks.
- **Boundary hygiene**: Minimal data marshalling; explicit conversions for paths/args.

Security notes:
- This wrapper does not sanitize commands. Only `pwsh()` uses literal quoting via `quote_pwsh_literal()`.
- Do *not* pass untrusted strings to `execute*` unless you quote/sanitize appropriately.
- Environment injection occurs via Config; avoid leaking secrets in logs/tracebacks.

Perf notes:
- All sync/async execution routes call into C++ directly. Python overhead is dominated by
  object creation and callback dispatch. Keep callbacks lean.
- Prefer batch/async when issuing many small commands to amortize round-trips.

Lifetime:
- `Shell.start()` ensures there is a running backend process. `Shell.stop()` tears it down.
- Context manager (`with Shell(...) as sh:`) guarantees stop-on-exit.

Compatibility:
- The C++ layer may expose both snake_case and camelCase fields; `ExecutionResult.from_cpp()`
  maps both to keep ABI compatibility.
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Dict, Optional, Callable, Any, Union
import concurrent.futures as cf


_CPP_MODULE: Any = None

import importlib
try:
    _CPP_MODULE = importlib.import_module(f"{__package__}._core")
except Exception as e:
    raise ImportError(
        "Failed to import the compiled extension 'virtualshell._core'. "
        "Make sure it was built and matches this Python/platform."
    ) from e


# Aliases to reduce attribute lookups on the hot path.
_CPP_VirtualShell = _CPP_MODULE.VirtualShell
_CPP_Config       = _CPP_MODULE.Config
_CPP_ExecResult   = _CPP_MODULE.ExecutionResult
_CPP_BatchProg    = _CPP_MODULE.BatchProgress


# ---------- Exceptions ----------
# Narrow, typed exceptions help callers implement precise retry/telemetry policies.
from .errors import (
    VirtualShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

# ---------- Utils ----------
def quote_pwsh_literal(s: str) -> str:
    """Return a PowerShell *single-quoted* literal for arbitrary text `s`.

    Rules:
    - Empty string => `''` (empty single-quoted literal)
    - Single quotes are doubled inside the literal per PowerShell rules.
    - No interpolation/expansion occurs within single quotes in PowerShell.

    This is safe for *data-as-argument* scenarios, not for embedding raw code.
    Use it to construct commands like: `Write-Output {literal}`.
    """
    if not s:
        return "''"
    out: List[str] = []
    append = out.append
    append("'")
    for ch in s:
        append("''" if ch == "'" else ch)
    append("'")
    return "".join(out)

def _effective_timeout(user_timeout: Optional[float], default_seconds: float) -> float:
    """Resolve an effective timeout (seconds).

    Priority:
    1) `user_timeout` if provided and > 0
    2) C++ config default (`default_seconds`)

    Always returns a float >= 0.0.
    """
    return float(user_timeout) if (user_timeout and user_timeout > 0) else float(default_seconds or 0.0)


def _raise_on_failure(
    res: _CPP_ExecResult,
    *,
    raise_on_error: bool,
    label: str,
    timeout_used: Optional[float],
) -> None:
    """Translate a C++ result into Python exceptions when requested.

    - If `res.success` is True: no-op.
    - Timeout heuristic: if `exit_code == -1` and the error string mentions "timeout",
      raise `ExecutionTimeoutError` with the effective timeout used.
    - Otherwise, if `raise_on_error` is True, raise `ExecutionError` with details.

    This keeps the default behavior non-throwing for bulk workflows while allowing
    strict error handling in critical paths.
    """
    if res.success:
        return
    err = (res.error or "")
    if res.exit_code == -1 and "timeout" in err.lower():
        raise ExecutionTimeoutError(f"{label} timed out after {timeout_used}s")
    if raise_on_error:
        msg = err if err else f"{label} failed with exit_code={res.exit_code}"
        raise ExecutionError(msg)


def _map_future(src_fut: cf.Future, mapper: Callable[[Any], Any]) -> cf.Future:
    """Create a new Future that maps the result of `src_fut` through `mapper`.

    - Preserves exception semantics: exceptions from `src_fut` or the mapper
      are propagated to the mapped future.
    - Avoids blocking the event loop / thread: uses `add_done_callback`.
    """
    nfut: cf.Future = cf.Future()

    def _done(f: cf.Future) -> None:
        try:
            nfut.set_result(mapper(f.result()))
        except Exception as e:  # Propagate mapping errors
            nfut.set_exception(e)

    src_fut.add_done_callback(_done)
    return nfut


# ---------- Python façade ----------
@dataclass(frozen=True)
class ExecutionResult:
    """Immutable Python-side execution result for ergonomic access.

    Mirrors fields from the C++ `ExecutionResult` (snake_case/camelCase tolerant):
    - output:      Captured STDOUT
    - error:       Captured STDERR or synthesized error text
    - exit_code:   Process/command exit code (convention: -1 often signals timeout)
    - success:     Normalized success flag provided by the backend
    - execution_time: Seconds (float) measured by the backend

    Use this class when you need a stable Pythonic type. If you require the raw C++
    object (e.g., for zero-copy interop), pass `as_dataclass=False` to public APIs.
    """
    out: str
    err: str
    exit_code: int
    success: bool
    execution_time: float

    @classmethod
    def from_cpp(cls, r: _CPP_ExecResult) -> "ExecutionResult":
        # Attribute access is defensive to tolerate ABI field name differences.
        return cls(
            out=getattr(r, "out", ""),
            err=getattr(r, "err", ""),
            exit_code=int(getattr(r, "exit_code", getattr(r, "exitCode", -1))),
            success=bool(getattr(r, "success", False)),
            execution_time=float(getattr(r, "execution_time", getattr(r, "executionTime", 0.0))),
        )


# ---------- Public API ----------
class Shell:
    """Thin ergonomic wrapper around the C++ VirtualShell.

    Notes:
    - No Python-side I/O; all execution flows delegate to the C++ backend.
    - All timeouts are best-effort and enforced by the backend. A timeout typically
      returns `exit_code == -1` and `success == False` with an error message.
    - Methods that accept `as_dataclass` can return the raw C++ result for callers
      that want to avoid extra allocations/mapping.
    """

    def __init__(
        self,
        powershell_path: Optional[str] = None,
        working_directory: Optional[Union[str, Path]] = None,
        timeout_seconds: float = 5.0,
        environment: Optional[Dict[str, str]] = None,
        initial_commands: Optional[List[str]] = None,
        cpp_module: Any = None,
    ) -> None:
        """Configure a new Shell instance.

        Parameters
        ----------
        powershell_path : Optional[str]
            Explicit path to `pwsh`/`powershell`. If omitted, the backend resolves it.
        working_directory : Optional[Union[str, Path]]
            Working directory for the child process. Resolved to an absolute path.
        timeout_seconds : float
            Default per-command timeout used when a method's `timeout` is not provided.
        environment : Optional[Dict[str, str]]
            Extra environment variables for the child process.
        initial_commands : Optional[List[str]]
            Commands that the backend will issue on process start (e.g., encoding setup).
        cpp_module : Any
            For testing/DI: provide a custom module exposing the C++ API surface.
        """
        mod = cpp_module or _CPP_MODULE
        cfg = mod.Config()
        if powershell_path:
            cfg.powershell_path = str(powershell_path)
        if working_directory:
            cfg.working_directory = str(Path(working_directory).resolve())
        cfg.timeout_seconds = int(timeout_seconds or 0)

        if environment:
            # Copy to detach from caller's dict and avoid accidental mutation.
            cfg.environment = dict(environment)
        if initial_commands:
            # Force string-ification to prevent surprises from non-str types.
            cfg.initial_commands = list(map(str, initial_commands))

        self._cfg: _CPP_Config = cfg
        self._core: _CPP_VirtualShell = mod.VirtualShell(cfg)

    def start(self) -> "Shell":
        """Start (or confirm) the backend PowerShell process.

        Returns self for fluent chaining.
        Raises `PowerShellNotFoundError` if the process cannot be started.
        """
        if self._core.is_alive():
            return self
        if self._core.start():
            return self

        # Backend could not start the process; provide a precise error.
        raise PowerShellNotFoundError(
            f"Failed to start PowerShell process. Path: '{self._cfg.powershell_path or 'pwsh/powershell'}'"
        )

    def stop(self, force: bool = False) -> None:
        """Stop the backend process.

        `force=True` requests an immediate termination (backend-specific semantics).
        Always safe to call; errors are wrapped in `SmartShellError`.
        """
        try:
            self._core.stop(force)
        except Exception as e:  # Surface backend failures in a consistent type.
            raise VirtualShellError(f"Failed to stop PowerShell: {e}") from e

    @property
    def is_running(self) -> bool:
        """Return True if the backend process is alive."""
        return bool(self._core.is_alive())

    # -------- sync --------
    def run(
        self,
        command: str,
        timeout: Optional[float] = None,
        *,
        raise_on_error: bool = False,
        as_dataclass: bool = True,
    ) -> Union[ExecutionResult, _CPP_ExecResult]:
        """Execute a single PowerShell command string.

        Parameters
        ----------
        command : str
            Raw PowerShell command. Caller is responsible for quoting/sanitizing.
        timeout : Optional[float]
            Per-call timeout override in seconds; <= 0 means "no override".
        raise_on_error : bool
            If True, raise `ExecutionTimeoutError`/`ExecutionError` on failure.
        as_dataclass : bool
            If True, return an `ExecutionResult`; otherwise return the raw C++ result.
        """
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)
        res: _CPP_ExecResult = self._core.execute(command=command, timeout_seconds=to)
        _raise_on_failure(res, raise_on_error=raise_on_error, label="Command", timeout_used=to)
        return ExecutionResult.from_cpp(res) if as_dataclass else res

    def run_script(
        self,
        script_path: Union[str, Path],
        args: Optional[Iterable[str]] = None,
        timeout: Optional[float] = None,
        *,
        dot_source: bool = False,
        raise_on_error: bool = False,
        as_dataclass: bool = True,
    ) -> Union[ExecutionResult, _CPP_ExecResult]:
        """Execute a script file with positional arguments.

        - `args` are passed as-is to the backend; quote appropriately for your script.
        - `dot_source=True` runs in the current context (if supported by the backend),
          which can mutate session state. Use with care.
        - `raise_on_error` only affects Python-side exception raising; the backend
          always runs with `raise_on_error=False` to avoid double-throwing.
        """
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)
        res: _CPP_ExecResult = self._core.execute_script(
            script_path=str(Path(script_path).resolve()),
            args=list(args or []),
            timeout_seconds=to,
            dot_source=bool(dot_source),
            raise_on_error=False,
        )
        _raise_on_failure(res, raise_on_error=raise_on_error, label="Script", timeout_used=to)
        return ExecutionResult.from_cpp(res) if as_dataclass else res

    def run_script_kv(
        self,
        script_path: Union[str, Path],
        named_args: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        *,
        dot_source: bool = False,
        raise_on_error: bool = False,
        as_dataclass: bool = True,
    ) -> Union[ExecutionResult, _CPP_ExecResult]:
        """Execute a script file with *named* arguments.

        `named_args` is copied to detach from caller mutations. Keys/values must be
        strings representable in the PowerShell context (caller ensures quoting).
        """
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)
        res: _CPP_ExecResult = self._core.execute_script_kv(
            script_path=str(Path(script_path).resolve()),
            named_args=dict(named_args or {}),
            timeout_seconds=to,
            dot_source=bool(dot_source),
            raise_on_error=False,
        )
        _raise_on_failure(res, raise_on_error=raise_on_error, label="ScriptKV", timeout_used=to)
        return ExecutionResult.from_cpp(res) if as_dataclass else res

    def run_batch(
        self,
        commands: Iterable[str],
        *,
        per_command_timeout: Optional[float] = None,
        stop_on_first_error: bool = True,
        as_dataclass: bool = True,
    ) -> List[Union[ExecutionResult, _CPP_ExecResult]]:
        """Execute multiple commands sequentially in the same session.

        - `per_command_timeout` applies to each individual command.
        - `stop_on_first_error` semantics are enforced by the backend.
        - Returns a list of results preserving the order of input commands.
        """
        to = _effective_timeout(per_command_timeout, self._cfg.timeout_seconds)
        vec = self._core.execute_batch(
            commands=list(commands),
            timeout_seconds=to,
        )
        if as_dataclass:
            return [ExecutionResult.from_cpp(r) for r in vec]
        return vec

    def run_async(self, command: str, callback: Optional[Callable[[ExecutionResult], None]] = None, *, as_dataclass: bool = True):
        """Asynchronously execute a single command.

        - If `callback` is provided, it is invoked on completion with the result type
          controlled by `as_dataclass`.
        - Returns a `concurrent.futures.Future` from the backend; if `as_dataclass` is True,
          the returned future is a mapped proxy that yields `ExecutionResult`.
        - Exceptions in your callback are swallowed to avoid breaking the executor.
        """
        def _cb(py_res: _CPP_ExecResult) -> None:
            if callback is None:
                return
            try:
                callback(ExecutionResult.from_cpp(py_res) if as_dataclass else py_res)
            except Exception:
                # Intentionally ignore to keep the executor stable.
                pass

        c_fut = self._core.execute_async(command=command, callback=_cb if callback else None)
        return _map_future(c_fut, lambda r: ExecutionResult.from_cpp(r)) if as_dataclass else c_fut

    def run_async_batch(
        self,
        commands: Iterable[str],
        progress: Optional[Callable[[ _CPP_BatchProg ], None]] = None,
        *,
        per_command_timeout: Optional[float] = None,
        stop_on_first_error: bool = True,
        as_dataclass: bool = True,
    ):
        """Asynchronously execute a batch of commands.

        - `progress` is a pass-through callback from the backend for per-command updates.
        - Returns a Future resolving to a list of results. Mapping to `ExecutionResult`
          is applied if `as_dataclass` is True.
        """
        to = _effective_timeout(per_command_timeout, self._cfg.timeout_seconds)
        fut = self._core.execute_async_batch(
            commands=list(commands),
            progress_callback=progress if progress else None,
            stop_on_first_error=bool(stop_on_first_error),
            per_command_timeout_seconds=to,
        )
        if as_dataclass:
            return _map_future(fut, lambda vec: [ExecutionResult.from_cpp(r) for r in vec])
        return fut

    def run_async_script(
        self,
        script_path: Union[str, Path],
        args: Optional[Iterable[str]] = None,
        callback: Optional[Callable[[ExecutionResult], None]] = None,
        *,
        timeout: Optional[float] = None,
        dot_source: bool = False,
        as_dataclass: bool = True,
    ):
        """Asynchronously execute a script with positional args.

        - `callback` is invoked (if provided) upon completion. Exceptions inside
          the callback are suppressed to protect the executor.
        - Returns a Future that yields either the raw C++ result or `ExecutionResult`.
        """
        def _cb(py_res: _CPP_ExecResult) -> None:
            if callback is None:
                return
            try:
                callback(ExecutionResult.from_cpp(py_res) if as_dataclass else py_res)
            except Exception:
                pass

        to = _effective_timeout(timeout, self._cfg.timeout_seconds)
        fut = self._core.execute_async_script(
            script_path=str(Path(script_path).resolve()),
            args=list(args or []),
            callback=_cb if callback else None,
            timeout_seconds=to,
            dot_source=bool(dot_source),
            raise_on_error=False,
        )
        if as_dataclass:
            return _map_future(fut, lambda r: ExecutionResult.from_cpp(r))
        return fut

    def run_async_script_kv(
        self,
        script_path: Union[str, Path],
        named_args: Optional[Dict[str, str]] = None,
        callback: Optional[Callable[[ExecutionResult], None]] = None,
        *,
        timeout: Optional[float] = None,
        dot_source: bool = False,
        as_dataclass: bool = True,
    ):
        """Asynchronously execute a script with named args.

        If `callback` is provided, it is wired to the returned Future using
        `add_done_callback` and receives the mapped result when available.
        """
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)
        fut = self._core.execute_async_script_kv(
            script_path=str(Path(script_path).resolve()),
            named_args=dict(named_args or {}),
            timeout_seconds=to,
            dot_source=bool(dot_source),
            raise_on_error=False,
        )
        if callback:
            def _done(f: cf.Future) -> None:
                try:
                    py_res = f.result()
                    callback(ExecutionResult.from_cpp(py_res) if as_dataclass else py_res)
                except Exception:
                    # Suppress to avoid breaking the executor.
                    pass
            try:
                fut.add_done_callback(_done)
            except Exception:
                # If the backend future doesn't support callbacks, silently continue.
                pass
        if as_dataclass:
            return _map_future(fut, lambda r: ExecutionResult.from_cpp(r))
        return fut

    # -------- convenience --------
    def pwsh(self, s: str, timeout: Optional[float] = None, raise_on_error: bool = False) -> ExecutionResult:
        """Execute a **literal** PowerShell string safely.

        Example:
            `shell.pwsh("Hello 'World'")` -> runs `Write-Output 'Hello ''World'''` semantics;
            here we only quote the literal; you still provide the full command.
        """
        return self.run(quote_pwsh_literal(s), timeout=timeout, raise_on_error=raise_on_error)

    def __enter__(self) -> "Shell":
        """Context manager entry: ensure backend is running."""
        if not self._core.is_alive():
            self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Context manager exit: stop backend regardless of errors in the block."""
        self.stop()
        return None