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
- Increase `timeout` per call or the session default `Shell(timeout=...)`.
- Consider `auto_restart_on_timeout=True` for long‑running commands.

**Encoding issues**
- `set_UTF8=True` is default; disable if you need legacy codepages.
- Ensure PowerShell `$OutputEncoding` matches expectations.

**The PowerShell host crashed (exit code -3)**
- A command that kills the host (e.g. a .NET stack overflow from a recursive `AssemblyResolve` handler) completes at once with `exit_code == -3` and `err` ending in `PowerShell process exited unexpectedly.`; the host's own last stderr lines come first, so read them.
- `is_running` is `False` afterwards; call `start()` to relaunch. Session state (variables, loaded assemblies) is gone, and `initial_commands` run again.

**"The term 'Get-FileHash' is not recognized" (Windows PowerShell 5.1)**
- Cmdlets that live in autoloaded modules (`Get-FileHash`, `Import-Csv`, ...) can fail to resolve transiently in 5.1 when many PowerShell processes start close together: its module analysis cache is rewritten concurrently. Not specific to virtualshell.
- Mitigation: import the module up front, e.g. `initial_commands=["Import-Module Microsoft.PowerShell.Utility"]`, or use the .NET API directly.

**Session restarts**
- Check `auto_restart_on_timeout` and `auto_restart_on_failure` settings.
- Save state before risky commands to recover after restarts.
