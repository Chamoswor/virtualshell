### Page: Configuration

`Shell(...)` common options:
- `powershell_path: Optional[str]` – explicit path to `pwsh`/`powershell.exe`; overrides `powershell_edition`
- `powershell_edition: str = "auto"` – which host to launch when no path is given:
  - `"auto"` – `pwsh` (PowerShell 7+) if it can be found, otherwise Windows PowerShell 5.1 on Windows
  - `"core"` – `pwsh` (PowerShell 7+), all platforms
  - `"desktop"` – Windows PowerShell 5.1 (`powershell.exe`); Windows only, raises `ValueError` elsewhere
  - aliases `"pwsh"` (core) and `"powershell"` (desktop) are accepted
- `working_directory: Optional[str|Path]` – child process CWD
- `timeout: float = 5.0` – default per‑command timeout in seconds, used when a call has no `timeout=` of its own (deprecated alias: `timeout_seconds`)
- `auto_restart_on_timeout: bool = True` – if `True`, backend restarts after timeout; Python side won’t raise timeout by default
- `environment: Optional[Dict[str,str]]` – extra env vars for the child
- `stdin_buffer_size: int = 65536` – size of the stdin pipe buffer in bytes
- `initial_commands: Optional[List[str]]` – send commands at process start
- `set_UTF8: bool = True` – set UTF‑8 output encoding in session
- `strip_results: bool = True` – trims leading/trailing whitespace from `.out` / `.err` on returned results, so `sh.run("1+1").out == "2"`; set `False` for the raw stream text
- `policy: Optional[ExecutionPolicy] = None` – guardrails (allow/deny lists, read-only lane, confirmation hook, auto `-WhatIf`); see [Agents & Guardrails](Agents-&-Guardrails)
- `max_output: Optional[int] = None` – default character budget for `run()` output; larger output is returned head+tail with a continuation marker for `fetch_output`
- `raise_on_error: bool = False` – session-wide default for the `raise_on_error` argument of `run` / `script`

`run()` and the other execution methods start the backend automatically when it is not running, so an explicit `start()` is optional (a context manager or `stop()` is still the way to shut it down).

**Inspecting the running host**
- `shell.configured_edition` – the `powershell_edition` value the Shell was built with
- `shell.edition` – `"core"` or `"desktop"`, as reported by `$PSVersionTable.PSEdition` (starts the backend if needed; cached until `stop()`)
- `shell.powershell_version` – e.g. `"7.5.3"` or `"5.1.22621.4391"`
- `shell.powershell_path` – the executable the backend actually launched

Every feature (sync/async execution, scripts, session snapshots, the zero-copy bridge, `make_proxy`, `generate_psobject`) works on both editions. The hosts still differ in language surface: Windows PowerShell 5.1 has no `$PSStyle`, no ternary / `??` operators, and prints errors in the classic multi-line format. Branch on `shell.edition` when you need PowerShell 7-only syntax.

**Utilities**
- `quote_pwsh_literal(s: str) -> str` – safely single‑quote arbitrary text for PowerShell (`from virtualshell import quote_pwsh_literal`)

**Threading**
- `Shell` is thread-safe, and the blocking calls (`run`, `script`, `start`, `stop`) release the GIL, so other Python threads keep running while PowerShell works. One Shell hosts one PowerShell process, which executes commands sequentially; create several Shells for genuinely parallel PowerShell.
