### Page: API Overview

**Core class**: `virtualshell.Shell`

**Common methods**
- `start() -> Shell` · `stop(force: bool=False) -> None`
- `is_running: bool` · `is_restarting: bool`
- `run(cmd: str|Iterable[str], timeout: float|None=None, raise_on_error=False) -> ExecutionResult | List[ExecutionResult]`
- `run_async(cmd: str|Sequence[str], callback=None, timeout: float|None=None) -> Future[...]`
- `script(script_path: str|Path, args: Iterable[str] | Dict[str,str] | None=None, timeout: float|None=None, dot_source=False, raise_on_error=False) -> ExecutionResult`
- `script_async(..., callback=None, timeout: float|None=None, dot_source=False) -> Future[ExecutionResult]`
- `pwsh(s: str, timeout: float|None=None, raise_on_error=False) -> ExecutionResult`  _(executes a **literal** string safely)_
- `save_session(timeout: float|None=None, raise_on_error=True) -> ExecutionResult`

**Properties**
- `python_run_id: str` · `session_path: Path`

**Result protocols**
- `ExecutionResult`: `.out`, `.err`, `.exit_code`, `.success`, `.execution_time`
- `BatchProgress` (async batch callbacks): `.currentCommand`, `.totalCommands`, `.lastResult`, `.isComplete`, `.allResults`

**Exceptions**
- `VirtualShellError`, `PowerShellNotFoundError`, `ExecutionTimeoutError`, `ExecutionError`
