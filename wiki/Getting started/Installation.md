### Page: Installation

**Pip**
```bash
pip install virtualshell
```

If you see an import error about the compiled extension, ensure the installed wheel matches your OS/arch and Python version.

**PowerShell host**
- By default (`powershell_edition="auto"`) the backend launches `pwsh` (PowerShell 7+) when it can be found on PATH or in its default install location, and otherwise falls back to Windows PowerShell 5.1 (`powershell.exe`) on Windows.
- Pin a host with `Shell(powershell_edition="core")` (pwsh) or `Shell(powershell_edition="desktop")` (Windows PowerShell 5.1, Windows only).
- Point at a specific executable with `Shell(powershell_path=...)`; it overrides the edition.
