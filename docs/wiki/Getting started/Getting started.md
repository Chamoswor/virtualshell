### Page: Getting Started

**Prerequisites**
- Python ≥ 3.10
- PowerShell available on PATH:
  - Recommended: PowerShell 7+ (`pwsh`)
  - Windows PowerShell also works (`powershell`)
- OS: Windows, Linux, or macOS

**Verify PowerShell**
```bash
pwsh -v   # or: powershell -v
```

**Install**
```bash
pip install virtualshell
```

**Hello world**
```python
from virtualshell import Shell

with Shell(timeout_seconds=5) as sh:
    res = sh.run("'Hello from PowerShell'")  # literal string prints itself
    print(res.out)  # -> Hello from PowerShell
```
