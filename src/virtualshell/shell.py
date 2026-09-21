"""High-level Python wrapper around the ``virtualshell._core`` extension.

This module provides a production-facing API that abstracts the underlying
PowerShell hosting engine implemented in C++. It handles configuration,
process lifecycle, synchronous/asynchronous execution, and error translation
into Python-friendly exceptions.
"""
from __future__ import annotations

import os
import secrets
import tempfile
import time
import concurrent.futures as cf
from pathlib import Path
from typing import Iterable, List, Dict, Optional, Callable, Any, Type, TypeVar, Union, cast, overload, Sequence, TYPE_CHECKING
from concurrent.futures import Future
from .generate_psobject import generate
from . import _module
from ._module import ExecutionResult, BatchProgress, Config, VirtualShell
from ._protocols import ExitCode

if TYPE_CHECKING:
    from .ps_proxy import PsProxy

_ProxyProtocol = TypeVar("_ProxyProtocol")

_IS_WINDOWS = os.name == "nt"

#: Canonical values for ``Shell(powershell_edition=...)``.
#: - "auto":    pwsh (PowerShell 7+) when found, otherwise Windows PowerShell on Windows
#: - "core":    pwsh (PowerShell 7+), all platforms
#: - "desktop": Windows PowerShell 5.1 (powershell.exe), Windows only
POWERSHELL_EDITIONS = ("auto", "core", "desktop")

_EDITION_ALIASES = {
    "auto": "auto",
    "core": "core",
    "pwsh": "core",
    "desktop": "desktop",
    "powershell": "desktop",
    "windows": "desktop",
}


def normalize_powershell_edition(value: Any) -> str:
    """Map a user-supplied edition name onto one of ``POWERSHELL_EDITIONS``.

    Accepts the canonical names plus the executable-style aliases ``pwsh``
    (core) and ``powershell`` / ``windows`` (desktop), case-insensitively.
    Raises ``ValueError`` for anything else.
    """
    text = str(value if value is not None else "auto").strip().lower()
    edition = _EDITION_ALIASES.get(text or "auto")
    if edition is None:
        raise ValueError(
            f"Invalid powershell_edition {value!r}; expected one of "
            + ", ".join(repr(e) for e in POWERSHELL_EDITIONS)
            + " (aliases: 'pwsh', 'powershell', 'windows')")
    return edition

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

def _strip_result_fields(res: ExecutionResult) -> ExecutionResult:
    """Trim trailing whitespace on ``out`` and ``err`` fields when they are strings."""
    if isinstance(res.out, str):
        res.out = res.out.strip()
    if isinstance(res.err, str):
        res.err = res.err.strip()
    return res


def _raise_on_failure(
    res: ExecutionResult,
    *,
    raise_on_error: bool,
    raise_on_timeout: bool = True,
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
    err = (res.err or "")
    err_lower = err.lower()
    if res.exit_code == -1 and raise_on_timeout and any(
        token in err_lower for token in ("timeout", "timed out", "time-out", "time out")
    ):
        raise ExecutionTimeoutError(f"{label} timed out after {timeout_used}s")
    if raise_on_error:
        msg = err if err else f"{label} failed with exit_code={res.exit_code}"
        raise ExecutionError(msg)

# ---------- Public API ----------
class Shell:
    def __init__(
        self,
        powershell_path: Optional[str] = None,
        working_directory: Optional[Union[str, Path]] = None,
        timeout_seconds: float = 5.0,
        auto_restart_on_timeout: bool = True,
        environment: Optional[Dict[str, str]] = None,
        stdin_buffer_size: int = 64 * 1024,
        initial_commands: Optional[List[str]] = None,
        set_UTF8: bool = True,
        strip_results: bool = False,
        powershell_edition: str = "auto",
        cpp_module: Any = None,
    ) -> None:
        """Configure a new Shell instance.

        Parameters
        ----------
        powershell_path : Optional[str]
            Explicit path to `pwsh`/`powershell`. If omitted, the backend resolves it
            from `powershell_edition`.
        working_directory : Optional[Union[str, Path]]
            Working directory for the child process. Resolved to an absolute path.
        timeout_seconds : float
            Default per-command timeout used when a method's `timeout` is not provided.
            default is 5.0.
        auto_restart_on_timeout : bool
            If True, the backend process is automatically restarted after a timeout.
            This is useful for long-running commands that may exceed the timeout. If False, the command will fail with a timeout error.
            default is True.
        environment : Optional[Dict[str, str]]
            Extra environment variables for the child process.
        stdin_buffer_size : int
            Size of the stdin pipe buffer in bytes. Default is 64 * 1024 (64 KiB).
        initial_commands : Optional[List[str]]
            Commands that the backend will issue on process start (e.g., encoding setup).
        set_UTF8 : bool
            If True, prepends a command to set `$OutputEncoding` to UTF-8. This is often desirable
            to avoid encoding issues with non-ASCII output. Disable if you need a different encoding.
            default is True.
        strip_results : bool
            If True, automatically strip leading/trailing whitespace from `out` and `err`, only
            when as_dataclass=True is used. Default is False.
        powershell_edition : str
            Which PowerShell to host when `powershell_path` is not given:
            ``"auto"`` (default) launches `pwsh` (PowerShell 7+) when it can be found and
            otherwise falls back to Windows PowerShell 5.1 on Windows; ``"core"`` always
            launches `pwsh`; ``"desktop"`` always launches Windows PowerShell 5.1
            (`powershell.exe`, Windows only - raises ValueError elsewhere). The aliases
            ``"pwsh"`` and ``"powershell"`` are accepted. Every feature (sync/async
            execution, scripts, session snapshots, the zero-copy bridge, proxies and
            stub generation) works on both editions; see `Shell.edition` for what is
            actually running.
        cpp_module : Any
            For testing/DI: provide a custom module exposing the C++ API surface.
        """
        mod = cpp_module or _module.core
        cfg: Config = mod.Config()
        if powershell_path:
            cfg.powershell_path = str(powershell_path)
        edition = normalize_powershell_edition(powershell_edition)
        if edition == "desktop" and not _IS_WINDOWS:
            raise ValueError(
                "powershell_edition='desktop' selects Windows PowerShell 5.1, which only "
                "exists on Windows; use 'core' (pwsh) or 'auto' on this platform")
        cfg.powershell_edition = edition
        if working_directory:
            cfg.working_directory = str(Path(working_directory).resolve())
        cfg.timeout_seconds = int(timeout_seconds or 0)
        cfg.auto_restart_on_timeout = bool(auto_restart_on_timeout)

        if environment:
            # Copy to detach from caller's dict and avoid accidental mutation.
            cfg.environment = dict(environment)
        cfg.stdin_buffer_size = int(stdin_buffer_size or 0)
        # Build the full list before assigning: `cfg.initial_commands` returns a
        # *copy* of the underlying C++ vector, so in-place mutation is lost.
        # Force string-ification to prevent surprises from non-str types.
        initial = list(map(str, initial_commands)) if initial_commands else []
        if set_UTF8:
            initial.insert(0, "$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()")
        if initial:
            cfg.initial_commands = initial

        module_dir = Path(__file__).resolve().parent
        self._restore_script_path = module_dir / "get-session.ps1"
        self._save_session_script_path = module_dir / "save-session.ps1"
        self._python_run_id = secrets.token_hex(16)
        # Store session snapshots in the system temp directory to avoid polluting caller paths.
        session_dir = Path(tempfile.gettempdir()) / "virtualshell"
        session_dir.mkdir(parents=True, exist_ok=True)
        self._session_path = session_dir / f"session_{self._python_run_id}.xml"
        cfg.restore_script_path = str(self._restore_script_path)
        cfg.session_snapshot_path = str(self._session_path)

        self._cfg: Config = cfg
        self._core: VirtualShell = mod.VirtualShell(cfg)
        self._zcb: Any = None
        self._strip_results = bool(strip_results)
        self._raise_on_timeout = not bool(auto_restart_on_timeout)
        self._pwsh_mem_init = False
        # Detected from the running host on first use; cleared by stop().
        self._edition: Optional[str] = None
        self._version: Optional[str] = None
        # proxy variable -> expression it was reached by (see PsProxy.ps_origin)
        self._proxy_origins: Dict[str, str] = {}
        # (runtime type, method, type args, arg count) -> session variable
        # holding the closed MethodInfo (see PsProxy.generic)
        self._generic_methods: Dict[Any, str] = {}
        self.pid: Optional[int] = None
    
    @property
    def python_run_id(self) -> str:
        """Return the random identifier used for this Python wrapper instance."""
        return self._python_run_id

    @property
    def session_path(self) -> Path:
        """Return the absolute path to the session snapshot XML file."""
        return self._session_path

    #--------- C++ module access ---------
    def get_config(self) -> Config:
        return self._cfg

    def update_config(self, cfg: Config) -> None:
        self._cfg = cfg
        self._core.update_config(cfg)
    
    def get_module(self) -> VirtualShell:
        return self._core.get_shared_ptr()

    def set_strip_results(self, v: bool) -> None:
        """Set whether to strip leading/trailing whitespace from `out` and `err`.

        Default is False. This only affects results returned via `as_dataclass=True`.
        """
        self._strip_results = bool(v)

    def start(self) -> "Shell":
        """Start (or confirm) the backend PowerShell process.

        Returns self for fluent chaining.
        Raises `PowerShellNotFoundError` if the process cannot be started.
        """
        if self._core.is_alive():
            
            return self
        if self._core.start():
            self.pid = self._core.get_process_id()
            return self

        # Backend could not start the process; provide a precise error.
        edition = self._cfg.powershell_edition
        target = self._cfg.powershell_path or {
            "core": "pwsh (PowerShell 7+)",
            "desktop": "Windows PowerShell 5.1 (powershell.exe)",
        }.get(edition, "pwsh, or Windows PowerShell on Windows")
        raise PowerShellNotFoundError(
            f"Failed to start PowerShell process. Path: '{target}' (powershell_edition='{edition}')"
        )

    def stop(self, force: bool = False) -> None:
        """Stop the backend process.

        `force=True` requests an immediate termination (backend-specific semantics).
        Always safe to call; errors are wrapped in `VirtualShellError`.
        """
        if self._zcb is not None:
            try:
                self._zcb.close()
            except Exception:
                pass
            self._zcb = None
        # A later start() may resolve a different executable; re-detect then.
        self._edition = None
        self._version = None
        self._proxy_origins.clear()
        self._generic_methods.clear()
        try:
            self._core.stop(force)
        except Exception as e:  # Surface backend failures in a consistent type.
            raise VirtualShellError(f"Failed to stop PowerShell: {e}") from e
        finally:
            try:
                if hasattr(self, "_session_path") and self._session_path.exists():
                    self._session_path.unlink()
            except OSError:
                pass

    @property
    def is_running(self) -> bool:
        """Return True if the backend process is alive."""
        return bool(self._core.is_alive())

    @property
    def is_restarting(self) -> bool:
        """Return True if the backend process is restarting."""
        return bool(self._core.is_restarting())

    @property
    def configured_edition(self) -> str:
        """The `powershell_edition` this Shell was configured with ('auto', 'core' or 'desktop')."""
        return str(self._cfg.powershell_edition)

    @property
    def edition(self) -> str:
        """Edition of the PowerShell host that is actually running.

        Returns ``"core"`` for PowerShell 7+ (`pwsh`) or ``"desktop"`` for Windows
        PowerShell 5.1, as reported by ``$PSVersionTable.PSEdition``. Starts the
        backend if needed; the answer is cached until `stop()`.
        """
        if self._edition is None:
            self._wait_if_restarting()
            if not self.is_running:
                self.start()
            detected = str(self._core.get_powershell_edition() or "").strip().lower()
            if detected not in ("core", "desktop"):
                raise VirtualShellError(
                    "Could not determine the edition of the running PowerShell host")
            self._edition = detected
        return self._edition

    @property
    def powershell_version(self) -> str:
        """Version of the running host (e.g. ``"7.5.3"`` or ``"5.1.22621.4391"``).

        Starts the backend if needed; cached until `stop()`.
        """
        if self._version is None:
            self._wait_if_restarting()
            if not self.is_running:
                self.start()
            version = str(self._core.get_powershell_version() or "").strip()
            if not version:
                return ""
            self._version = version
        return self._version

    @property
    def powershell_path(self) -> str:
        """Executable the backend launched (resolved on `start()`).

        Before the first start this is the configured `powershell_path`, which is
        empty when the executable is chosen from `powershell_edition`.
        """
        resolved = ""
        if self.is_running:
            resolved = str(self._core.get_resolved_powershell_path() or "")
        return resolved or str(self._cfg.powershell_path)

    @overload
    def run(self, cmd: str, *, timeout: Optional[float]=..., raise_on_error: bool=...) -> ExecutionResult: ...
    """Execute a single PowerShell command."""

    @overload
    def run(self, cmd: List[str], *, timeout: Optional[float]=..., raise_on_error: bool=...) -> List[ExecutionResult]: ...
    """Execute a batch of PowerShell commands."""

    # -------- sync --------
    def run(
        self,
        cmd: Union[str, Iterable[str]],
        *,
        timeout: Optional[float] = None,
        raise_on_error: bool = False,
    ) -> Union[ExecutionResult, List[ExecutionResult]]:
        self._wait_if_restarting()
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)

        if isinstance(cmd, (list, tuple)):
            commands = [str(c) for c in cmd]
            vec = self._core.execute_batch(commands=commands, timeout_seconds=to)
            if raise_on_error:
                for idx, r in enumerate(vec):
                    _raise_on_failure(r, raise_on_error=True,
                                    raise_on_timeout=self._raise_on_timeout,
                                    label=f"Command[{idx}]", timeout_used=to)
            return [_strip_result_fields(r) for r in vec] if self._strip_results else vec

        # Single command
        command = str(cmd)
        res: ExecutionResult = self._core.execute(command=command, timeout_seconds=to)
        _raise_on_failure(res, raise_on_error=raise_on_error,
                        raise_on_timeout=self._raise_on_timeout,
                        label="Command", timeout_used=to)
        return _strip_result_fields(res) if self._strip_results else res
    

    # --- run_async ---
    @overload
    def run_async(self, cmd: str, *, callback: Optional[Callable[[ExecutionResult], None]]=..., timeout: Optional[float]=...) -> Future[ExecutionResult]: ...
    """Execute a single PowerShell command asynchronously."""

    @overload
    def run_async(self, cmd: List[str], *, callback: Optional[Callable[[BatchProgress], None]]=..., timeout: Optional[float]=...) -> Future[List[ExecutionResult]]: ...
    """Execute a batch of PowerShell commands asynchronously."""
    
    def run_async(self, cmd: Union[str, Sequence[str]], *, callback=None, timeout: Optional[float]=None) -> Union[Future[ExecutionResult], Future[List[ExecutionResult]]]:
        """Dispatch commands asynchronously using the C++ executor and return a Python ``Future``.

        When ``callback`` is provided we invoke it on completion (single command) or during
        progress events (batch execution). Any exception raised by the callback is suppressed to
        avoid destabilising the worker threads, matching the behaviour of common executor APIs.
        """

        self._wait_if_restarting()
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)

        def _safe_call(cb, arg):
            # Guard user callbacks against exceptions so the executor can continue safely.
            if cb is None:
                return
            try:
                cb(arg)
            except Exception:
                pass

        if isinstance(cmd, (list, tuple)):
            commands = [str(c) for c in cmd]

            prog_cb = (lambda p: _safe_call(callback, p)) if callback else None
            return self._core.execute_async_batch(
                commands=commands,
                progress_callback=prog_cb,
                stop_on_first_error=True,
                per_command_timeout_seconds=to,
            )

        command = str(cmd)
        res_cb = (lambda r: _safe_call(callback, r)) if callback else None
        return self._core.execute_async(
            command=command,
            callback=res_cb,
            timeout_seconds=to,
        )
        
    def script(
        self,
        script_path: Union[str, Path],
        args: Optional[Union[Iterable[str], Dict[str, str]]] = None,
        *,
        timeout: Optional[float] = None,
        dot_source: bool = False,
        raise_on_error: bool = False,
    ) -> ExecutionResult:
        """Execute a script file with positional arguments.
        - `script_path` is the path to the script file to execute.
        - `args` is either a list of positional arguments or a dict of named arguments.
        - `dot_source=True` runs in the current context (if supported by the backend),
          which can mutate session state. Use with care.
        - `raise_on_error` only affects Python-side exception raising; the backend
          always runs with `raise_on_error=False` to avoid double-throwing.
        """
        self._wait_if_restarting()
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)

        if isinstance(args, dict) and args is not None:
            # Named args path.
            named_args = dict(args)
            res: ExecutionResult = self._core.execute_script_kv(
                script_path=str(Path(script_path).resolve()),
                named_args=named_args,
                timeout_seconds=to,
                dot_source=bool(dot_source),
                raise_on_error=False,
            )
            _raise_on_failure(res, raise_on_error=raise_on_error, raise_on_timeout=self._raise_on_timeout, label="ScriptKV", timeout_used=to)

            return _strip_result_fields(res) if self._strip_results else res
        
        res = self._core.execute_script(
            script_path=str(Path(script_path).resolve()),
            args=list(args or []) ,
            timeout_seconds=to,
            dot_source=bool(dot_source),
            raise_on_error=False,
        )
        _raise_on_failure(res, raise_on_error=raise_on_error, raise_on_timeout=self._raise_on_timeout, label="Script", timeout_used=to)

        return _strip_result_fields(res) if self._strip_results else res
    
    def script_async(
        self,
        script_path: Union[str, Path],
        args: Optional[Union[Iterable[str], Dict[str, str]]] = None,
        callback: Optional[Callable[[ExecutionResult], None]] = None,
        *,
        timeout: Optional[float] = None,
        dot_source: bool = False,
    ) -> Future[ExecutionResult]:

        self._wait_if_restarting()
        to = _effective_timeout(timeout, self._cfg.timeout_seconds)

        if isinstance(args, dict) and args is not None:
            # Named args path.
            named_args = dict(args)
            fut = self._core.execute_async_script_kv(
                script_path=str(Path(script_path).resolve()),
                named_args=named_args,
                timeout_seconds=to,
                dot_source=bool(dot_source),
                raise_on_error=False,
            )
            if callback:
                def _done(f: cf.Future) -> None:
                    try:
                        py_res = f.result()
                        callback(py_res)
                    except Exception:
                        # Suppress to avoid breaking the executor.
                        pass
                try:
                    fut.add_done_callback(_done)
                except Exception:
                    # If the backend future doesn't support callbacks, silently continue.
                    pass
            return fut

        def _cb(py_res: ExecutionResult) -> None:
            if callback is None:
                return
            try:
                callback(py_res)
            except Exception:
                pass
        fut = self._core.execute_async_script(
            script_path=str(Path(script_path).resolve()),
            args=list(args or []),
            callback=_cb if callback else None,
            timeout_seconds=to,
            dot_source=bool(dot_source),
            raise_on_error=False,
        )
        return fut

    # -------- convenience --------
    def save_session(
        self,
        timeout: Optional[float] = None,
        *,
        raise_on_error: bool = True,
    ) -> ExecutionResult:
        """Persist the current session state via the bundled save-session.ps1."""
        return self.script(
            self._save_session_script_path,
            args=[str(self._session_path)],
            timeout=timeout,
            dot_source=False,
            raise_on_error=raise_on_error,
        )

    
    if TYPE_CHECKING:
        from .zero_copy_bridge_shell import ZeroCopyBridge
        def zero_copy_bridge(self, frame_mb: int = 16, chunk_mb: int = 4) -> ZeroCopyBridge:
            """Return this shell's shared ZeroCopyBridge, creating it on first use.

            The PowerShell side holds a single channel per session, so all users
            of a Shell must share one bridge. It is closed automatically by
            `stop()`.
            """
            ...
    else:
        def zero_copy_bridge(self, frame_mb: int = 16, chunk_mb: int = 4) -> Any:
            if self._zcb is None or getattr(self._zcb, "_channel", None) is None:
                from .zero_copy_bridge_shell import ZeroCopyBridge
                self._zcb = ZeroCopyBridge(self, frame_mb=frame_mb, chunk_mb=chunk_mb)
            return self._zcb

    @overload
    def make_proxy(self, type_name: str, obj_ref: Optional[str] = None,
                   *, static: bool = False) -> "PsProxy": ...

    @overload
    def make_proxy(self, type_name: Type[_ProxyProtocol], obj_ref: Optional[str] = None,
                   *, static: bool = False) -> _ProxyProtocol: ...

    def make_proxy(self, type_name: Union[str, Type[Any]], obj_ref: Optional[str] = None,
                   *, static: bool = False) -> Any:
        """Create a live proxy for a PowerShell object.

        Two call forms:

        - ``make_proxy("", obj_ref)`` where `obj_ref` is an existing variable
          ("$client") or a creation expression ("System.Text.StringBuilder(32)").
        - ``make_proxy(GeneratedProtocol)`` where the class was produced by
          `generate_psobject`: the embedded ``__ps_expression__`` metadata
          recreates the object, and the return value is typed as the protocol
          so no annotation is needed. Pass `obj_ref` (e.g. "$existing") to
          bind an existing variable instead of creating a new object; an
          empty `obj_ref` means the same as omitting it.

        A bare type literal such as ``make_proxy("", "[System.IO.Path]")``
        yields a *static* proxy exposing the type's static methods,
        properties and constants. `static=True` forces static mode for an
        unbracketed type name or a ``$variable`` that holds a type object.
        """
        from .ps_proxy import PsProxy, ensure_assembly_loaded

        if isinstance(type_name, type):
            proto = type_name
            ps_type = str(getattr(proto, "__ps_type_name__", "") or "")
            expression = str(getattr(proto, "__ps_expression__", "") or "")
            if not ps_type and not expression:
                raise TypeError(
                    f"{proto.__name__} carries no __ps_type_name__/__ps_expression__ "
                    "metadata; regenerate it with generate_psobject")
            if not expression and not obj_ref:
                # Package stubs for non-root types: reached through a parent.
                raise TypeError(
                    f"{proto.__name__} ({ps_type}) has no creation expression: objects of "
                    "this type are obtained from a parent object (e.g. tia.Projects), or "
                    "bind one with obj_ref='$variable' / '$parent.Member'")
            expression = expression or ps_type
            is_static = static or bool(getattr(proto, "__ps_static__", False))
            # Types from outside the runtime (recorded by generate_psobject)
            # need their assembly loaded before the expression can run.
            assembly = str(getattr(proto, "__ps_assembly__", "") or "")
            if assembly:
                ensure_assembly_loaded(
                    self, assembly, str(getattr(proto, "__ps_assembly_name__", "") or ""))
            # None and "" both mean "use the stub's own expression".
            return PsProxy(self, ps_type, obj_ref or expression, static=is_static)

        return PsProxy(self, type_name, obj_ref if obj_ref is not None else "$obj",
                       static=static)

    def generate_psobject(self, command: Union[str, "PsProxy"], output_path: Path, *,
                          expression: Optional[str] = None,
                          follow: bool = False,
                          include_namespaces: Optional[Sequence[str]] = None,
                          max_types: int = 2000) -> Optional[List[Path]]:
        """Generate a typed Python ``Protocol`` for the object `command` yields.

        `command` is a PowerShell expression, a ``$variable``, a bare type
        literal (``"[Vendor.Sdk.Root]"``), or a `PsProxy`.

        With ``follow=True``, `output_path` is a *package directory*: the
        type graph reachable from the root through properties, indexers,
        method returns and parameters is walked by reflection, and one module
        per type is written, cross-annotated with each other (so
        ``tia.Projects`` is typed ``ProjectComposition`` and
        ``projects.Item(0)`` is typed ``Project``, with completion all the way
        down). Only types outside the .NET runtime (the SDK) are followed;
        runtime types map to Python scalars or ``Any`` and enums to ``str``.
        `include_namespaces` (e.g. ``["Siemens.Engineering"]``) restricts the
        walk to those namespaces and the namespaces nested under them; an
        empty list follows every SDK type. `max_types` caps the walk. Returns
        the written module paths. Only the root module carries
        ``__ps_expression__``; other types are reached via a parent.

        The generated class embeds what `make_proxy(GeneratedClass)` needs to
        reach the object again in another session: the type name, an
        expression, and, for types outside the .NET runtime, the assembly path
        to load first. The expression is, in order of preference: `expression`
        if given; the proxy's `ps_origin` (e.g. ``"$tia.Projects"`` for an
        object obtained through a parent proxy, also found when `command` is
        such a proxy's `ps_ref`); ``[Type]::new()`` for a ``$variable`` whose
        type has a parameterless constructor; else `command` itself. Pass
        `expression` when none of those can recreate the object, e.g.
        ``"[Siemens.Engineering.TiaPortal]::new([Siemens.Engineering.TiaPortalMode]::WithoutUserInterface)"``.
        """
        if follow:
            from .generate_package import generate_package
            return generate_package(self, command, Path(output_path), expression=expression,
                                    include_namespaces=(list(include_namespaces)
                                                        if include_namespaces else None),
                                    max_types=int(max_types))
        generate(self, command, output_path, expression=expression)
        return None

    def _register_proxy_origin(self, ref: str, origin: str) -> None:
        """Called by PsProxy: remember how proxy variable `ref` was reached."""
        self._proxy_origins[ref] = origin

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

    def __repr__(self) -> str:
        return f"<Shell running={int(self.is_running)}>"

    def __exit__(self, exc_type, exc, tb) -> None:
        """Context manager exit: stop backend regardless of errors in the block."""
        self.stop()
        return None
    
    def _wait_if_restarting(self, poll_interval: float = 0.1) -> None:
        """Block if the backend process has restarted.

        - Only relevant if `auto_restart_on_timeout=True`.
        - Polls `is_restarting` every `poll_interval` seconds.
        - Use with care: this is a blocking call that may wait indefinitely.
        """
        while self.is_restarting:
            time.sleep(poll_interval)
        return