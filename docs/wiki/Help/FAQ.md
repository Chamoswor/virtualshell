### Page: FAQ

- **Windows PowerShell vs PowerShell 7?** Both can work; 7+ is recommended.
- **How to run commands asynchronously?** Use `run_async()` to get a `Future` for non-blocking execution.
- **Can I keep state between commands?** Yes; the backend process persists session state until stopped.
- **Where is the session saved?** `save_session()` writes an XML snapshot; see `session_path`.
- **What about timeouts?** Use the `timeout` parameter per call or set a default via `timeout_seconds`.
- **How to handle long-running commands?** Consider `auto_restart_on_timeout=True` to recover state after timeouts.
- **What if PowerShell is not found?** Ensure `pwsh`/`powershell` is on PATH or set `powershell_path`.
- **How to fix ImportError for compiled extensions?** Ensure the wheel matches your OS/arch/Python; try a clean venv and reinstall.
- **How to deal with encoding issues?** `set_UTF8=True` is default; disable if you need legacy codepages.