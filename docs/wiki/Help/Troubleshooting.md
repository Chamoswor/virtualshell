### Page: Troubleshooting

**ImportError: failed to import compiled extension**
- Ensure the installed wheel matches your OS/arch/Python.
- Try a clean venv and reinstall.

**PowerShell not found / cannot start**
- Verify `pwsh`/`powershell` on PATH; optionally set `powershell_path`.

**Timeouts**
- Increase `timeout` per call or `timeout_seconds` default.
- Consider `auto_restart_on_timeout=True` for long‑running commands.

**Encoding issues**
- `set_UTF8=True` is default; disable if you need legacy codepages.
- Ensure PowerShell `$OutputEncoding` matches expectations.

**Session restarts**
- Check `auto_restart_on_timeout` and `auto_restart_on_failure` settings.
- Save state before risky commands to recover after restarts.
