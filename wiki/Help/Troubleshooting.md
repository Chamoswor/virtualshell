### Page: Troubleshooting

**ImportError: failed to import compiled extension**
- Ensure the installed wheel matches your OS/arch/Python.
- Try a clean venv and reinstall.

**PowerShell not found / cannot start**
- Verify `pwsh` is on PATH (or, on Windows, that Windows PowerShell 5.1 is installed); optionally set `powershell_path`.
- `powershell_edition="desktop"` only works on Windows; use `"core"` or `"auto"` elsewhere.
- The `PowerShellNotFoundError` message names the edition and executable that was attempted.

**Command works in pwsh but fails in Windows PowerShell**
- Check `shell.edition`: with the default `"auto"` you get Windows PowerShell 5.1 when `pwsh` is missing, and 5.1 lacks PowerShell 7 syntax (`?:`, `??`, `$PSStyle`, ...). Install pwsh or pin `powershell_edition="core"`.

**Timeouts**
- Increase `timeout` per call or `timeout_seconds` default.
- Consider `auto_restart_on_timeout=True` for long‑running commands.

**Encoding issues**
- `set_UTF8=True` is default; disable if you need legacy codepages.
- Ensure PowerShell `$OutputEncoding` matches expectations.

**Session restarts**
- Check `auto_restart_on_timeout` and `auto_restart_on_failure` settings.
- Save state before risky commands to recover after restarts.
