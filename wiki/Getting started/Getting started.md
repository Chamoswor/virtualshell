### Page: Getting Started

**Prerequisites**
- Python ≥ 3.10
- A PowerShell host:
  - Recommended: PowerShell 7+ (`pwsh`) on PATH – all platforms
  - Windows PowerShell 5.1 (`powershell.exe`) – Windows only, used automatically when `pwsh` is missing, or on request via `Shell(powershell_edition="desktop")`
- OS: Windows, Linux, or macOS

**Verify PowerShell**
```bash
pwsh -v   # or, on Windows: powershell -NoProfile -Command '$PSVersionTable.PSVersion'
```

**Install**
```bash
pip install virtualshell
```

**Hello world**
```python
from virtualshell import Shell

with Shell(timeout=5) as sh:
    res = sh.run("'Hello from PowerShell'")  # literal string prints itself
    print(res.out)  # -> Hello from PowerShell
```
