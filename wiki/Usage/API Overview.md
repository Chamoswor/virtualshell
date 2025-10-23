### Page: API Overview

**Core class**: `virtualshell.Shell`

**Shell instantiation**

- `Shell(powershell_path: str|None=None, working_directory: str|Path|None=None, timeout_seconds: float=5.0, auto_restart_on_timeout: bool=True, environment: Optional[Dict[str,str]]=None, stdin_buffer_size: int=65536, initial_commands: Optional[List[str]]=None, set_UTF8: bool=True, strip_results: bool=False) -> Shell`
    - Create a new PowerShell shell controller.
    - See [Configuration](#configuration) for parameter details.

**Common methods**
- `start() -> Shell` · `stop(force: bool=False) -> None`
- `is_running: bool` · `is_restarting: bool`
- `run(cmd: str|Iterable[str], timeout: float|None=None, raise_on_error=False) -> ExecutionResult | List[ExecutionResult]`
- `run_async(cmd: str|Sequence[str], callback=None, timeout: float|None=None) -> Future[...]`
- `script(script_path: str|Path, args: Iterable[str] | Dict[str,str] | None=None, timeout: float|None=None, dot_source=False, raise_on_error=False) -> ExecutionResult`
- `script_async(..., callback=None, timeout: float|None=None, dot_source=False) -> Future[ExecutionResult]`
- `pwsh(s: str, timeout: float|None=None, raise_on_error=False) -> ExecutionResult`  _(executes a **literal** string safely)_
- `save_session(timeout: float|None=None, raise_on_error=True) -> ExecutionResult`
- `restore_session(snapshot_path: str|Path, timeout: float|None=None, raise_on_error=True) -> ExecutionResult`
- `make_proxy(type_name: str, object_expression: str, *, depth: int=4) -> Any`
- `generate_psobject(type_expression: str, output_path: str|Path) -> None`

**Properties**
- `python_run_id: str` · `session_path: Path`

**Result protocols**
- `ExecutionResult`: `.out`, `.err`, `.exit_code`, `.success`, `.execution_time`
- `BatchProgress` (async batch callbacks): `.currentCommand`, `.totalCommands`, `.lastResult`, `.isComplete`, `.allResults`

**Exceptions**
- `VirtualShellError`, `PowerShellNotFoundError`, `ExecutionTimeoutError`, `ExecutionError`
