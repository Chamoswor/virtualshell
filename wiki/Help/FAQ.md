### Page: FAQ

- **Windows PowerShell vs PowerShell 7?** Both are fully supported. `Shell()` defaults to `powershell_edition="auto"`, which launches `pwsh` (7+) when it can be found and otherwise falls back to Windows PowerShell 5.1 on Windows. Pin one with `powershell_edition="core"` or `"desktop"`, and check `shell.edition` to see which is running. PowerShell 7 is still recommended for its newer language features.
- **How to run commands asynchronously?** Use `run_async()` to get a `Future` for non-blocking execution.
- **Can I keep state between commands?** Yes; the backend process persists session state until stopped.
- **Where is the session saved?** `save_session()` writes an XML snapshot; see `session_path`.
- **What about timeouts?** Use the `timeout` parameter per call or set a session default via `Shell(timeout=...)`.
- **How to handle long-running commands?** Consider `auto_restart_on_timeout=True` to recover state after timeouts.
- **What if PowerShell is not found?** Ensure `pwsh` is on PATH (or Windows PowerShell is installed, on Windows), pick an edition with `powershell_edition`, or set `powershell_path` explicitly.
- **How to fix ImportError for compiled extensions?** Ensure the wheel matches your OS/arch/Python; try a clean venv and reinstall.
- **How to deal with encoding issues?** `set_UTF8=True` is default; disable if you need legacy codepages.
