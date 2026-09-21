### Page: API Overview

**Core class**: `virtualshell.Shell`

**Shell instantiation**

- `Shell(powershell_path: str|None=None, working_directory: str|Path|None=None, timeout_seconds: float=5.0, auto_restart_on_timeout: bool=True, environment: Optional[Dict[str,str]]=None, stdin_buffer_size: int=65536, initial_commands: Optional[List[str]]=None, set_UTF8: bool=True, strip_results: bool=True, powershell_edition: str="auto", policy: ExecutionPolicy|None=None, max_output: int|None=None, raise_on_error: bool=False) -> Shell`
    - Create a new PowerShell shell controller.
    - See [Configuration](#configuration) for parameter details.
    - `run()` and friends start the backend automatically; an explicit `start()` is optional.

**Common methods**
- `start() -> Shell` · `stop(force: bool=False) -> None`
- `is_running: bool` · `is_restarting: bool`
- `run(cmd: str|Iterable[str], timeout: float|None=None, raise_on_error: bool|None=None, max_output: int|None=None) -> ExecutionResult | List[ExecutionResult]`
- `run_async(cmd: str|Sequence[str], callback=None, timeout: float|None=None) -> Future[...]`
- `run_objects(cmd: str, select: str|Sequence[str]|None=None, first: int|None=None, depth: int=2, timeout: float|None=None, raise_on_error=True) -> list`  _(structured results via ConvertTo-Json; see [Agents & Guardrails](Agents-&-Guardrails))_
- `fetch_output(key: str, offset: int=0, max_output: int|None=None) -> OutputSlice`  _(page through output truncated by a `max_output` budget)_
- `script(script_path: str|Path, args: Iterable[str] | Dict[str,str] | None=None, timeout: float|None=None, dot_source=False, raise_on_error: bool|None=None) -> ExecutionResult`
- `script_async(..., callback=None, timeout: float|None=None, dot_source=False) -> Future[ExecutionResult]`
- `pwsh(s: str, timeout: float|None=None, raise_on_error: bool|None=None) -> ExecutionResult`  _(executes a **literal** string safely)_
- `save_session(timeout: float|None=None, raise_on_error=True) -> ExecutionResult`
- `checkpoint(name: str|None=None) -> Checkpoint` · `restore(checkpoint: Checkpoint|str|None=None) -> ExecutionResult` · `checkpoints: tuple[Checkpoint, ...]`
- `interrupt(restore: bool=True) -> bool`  _(abort the running command now; fast restart + state reload from the newest snapshot)_
- `command_schema(name: str, include_help=True) -> dict` · `module_schemas(module: str, include_help=True) -> list[dict]`  _(MCP-style tool definitions from Get-Command metadata)_
- `make_proxy(type_name: str, obj_ref: str = "$obj", *, static: bool = False) -> PsProxy`  _(live object proxy; `obj_ref` is a `$variable`, a creation expression, or a `[Type]` literal for a static proxy)_
- `generate_psobject(type_expression: str, output_path: str|Path) -> None`
- `zero_copy_bridge(frame_mb: int = 16, chunk_mb: int = 4) -> ZeroCopyBridge`  _(shared per-shell bridge, created lazily)_

**Properties**
- `python_run_id: str` · `session_path: Path` · `policy: ExecutionPolicy|None` _(swap guardrails at runtime)_

**Result protocols**
- `ExecutionResult`: `.out`, `.err`, `.exit_code`, `.success`, `.execution_time`
- `BatchProgress` (async batch callbacks): `.currentCommand`, `.totalCommands`, `.lastResult`, `.isComplete`, `.allResults`
- `OutputSlice` (from `fetch_output`): `.text`, `.offset`, `.next_offset`, `.total_chars`, `.total_lines`, `.at_end`

**Exceptions**
- `VirtualShellError`, `PowerShellNotFoundError`, `ExecutionTimeoutError`, `ExecutionError`
- `PromptBlockedError` _(a command tried to prompt for interactive input)_, `PolicyViolationError` _(blocked by the shell's `ExecutionPolicy`)_
