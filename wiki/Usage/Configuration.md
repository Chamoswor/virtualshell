### Page: Configuration

`Shell(...)` common options:
- `powershell_path: Optional[str]` – override pwsh/powershell path
- `working_directory: Optional[str|Path]` – child process CWD
- `timeout_seconds: float = 5.0` – default per‑command timeout
- `auto_restart_on_timeout: bool = True` – if `True`, backend restarts after timeout; Python side won’t raise timeout by default
- `environment: Optional[Dict[str,str]]` – extra env vars for the child
- `initial_commands: Optional[List[str]]` – send commands at process start
- `set_UTF8: bool = True` – set UTF‑8 output encoding in session
- `strip_results: bool = False` – when `True`, trims whitespace for `.out` / `.err` on returned results (dataclass path)

**Utilities**
- `quote_pwsh_literal(s: str) -> str` – safely single‑quote arbitrary text for PowerShell
