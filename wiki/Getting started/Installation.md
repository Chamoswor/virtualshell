### Page: Installation

**Pip**
```bash
pip install virtualshell
```

If you see an import error about the compiled extension, ensure the installed wheel matches your OS/arch and Python version.

**PowerShell path**
- The backend auto‑detects `pwsh`/`powershell`. You can override with `Shell(powershell_path=...)`.