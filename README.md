# 🐚 virtualshell
[![PyPI version](https://img.shields.io/pypi/v/virtualshell.svg)](https://pypi.org/project/virtualshell/)
[![Python versions](https://img.shields.io/pypi/pyversions/virtualshell.svg)](https://pypi.org/project/virtualshell/)
[![OS](https://img.shields.io/badge/OS-Windows%20%7C%20Linux%20%7C%20macOS-blue)](#)
[![License](https://img.shields.io/github/license/Chamoswor/virtualshell.svg)](LICENSE)
[![Build](https://img.shields.io/github/actions/workflow/status/Chamoswor/virtualshell/workflow.yml)](https://github.com/Chamoswor/virtualshell/actions)


**High-performance PowerShell automation for Python**
Run PowerShell commands with *millisecond latency*, persistent sessions, async execution, and a zero-copy bridge — without juggling subprocesses.

> 🔗 **Full documentation lives in the [wiki](https://github.com/Chamoswor/virtualshell/wiki)**
> This README focuses on *what it is*, *why it matters*, and *how to get started fast*.

## ✨ What is virtualshell?

`virtualshell` keeps **one PowerShell host warm** behind a thin Python wrapper powered by a **C++ engine**.

Instead of spawning `pwsh` for every command, you get:

* a **persistent PowerShell session**
* **~2–4 ms execution latency**
* **async + batch execution**
* **structured, predictable results**
* optional **zero-copy shared memory** (Windows)

Perfect for:

* Python orchestration layers
* long-running agents
* CI/test harnesses
* automation tooling that must be *fast and reliable*

## 🚀 Why use it?

* 🔄 **Persistent session**
  Reuse loaded modules, `$env:*`, functions, and global state.

* ⚡ **Ultra-low latency**
  Avoid the ~200 ms cost of `subprocess.run("pwsh")`.

* 🔀 **Async & batching**
  Schedule concurrent commands with timeouts and callbacks.

* 📊 **Structured results**
  Every call returns stdout, stderr, exit code, timing, and success state.

* 🚨 **Predictable failures**
  Typed Python exceptions for timeouts, missing PowerShell, execution errors.

* 🛠️ **Type-safe automation**
  Generate Python `Protocol`s from PowerShell objects and control them via live proxies.

## 📦 Installation

Pre-built wheels are published for:

* **Windows**
* **Linux** (x86_64 / aarch64)
* **macOS** (universal2)

### PowerShell requirement

PowerShell (`pwsh` or `powershell.exe`) must be available on `PATH`,
unless you pass an explicit path in the configuration.

---

### ✅ Recommended: install pre-built wheels

```bash
pip install virtualshell
```

No compiler or build tools required.

---

### 🔧 Build from source (optional)

Use this only if you explicitly want to build locally.

#### Windows (⚠️ 64-bit only)

1. Install **Visual Studio Build Tools**

   * Workload: *Desktop development with C++*
2. Open **x64 Native Tools Command Prompt for VS**

---

```bash
pip install virtualshell --no-binary virtualshell
```

Or directly from GitHub:

```bash
pip install "git+https://github.com/Chamoswor/virtualshell"
```

---

### 🔍 Verify installation

```bash
python -c "import virtualshell; print('virtualshell OK')"
```

---

## ⚡ Quick start

```python
from virtualshell import Shell

with Shell(timeout_seconds=5) as sh:
    print(sh.run("Write-Output 'Hello from pwsh'").out.strip())

    sh.run("function Inc { $global:i++; $global:i }")
    print(sh.run("Inc").out.strip())  # 1
    print(sh.run("Inc").out.strip())  # 2
```

### Async execution

```python
from virtualshell import Shell
import asyncio

async def main():
    shell = Shell().start()
    fut = shell.run_async("Get-Date")
    res = await asyncio.wrap_future(fut)
    print(res.out.strip())
    shell.stop()

asyncio.run(main())
```

### Running scripts with arguments

```python
from pathlib import Path
from virtualshell import Shell

shell = Shell().start()

shell.script(Path("test.ps1"), args=["alpha", "42"])
shell.script(Path("test.ps1"), args={"Name": "Alice", "Count": "3"})

shell.stop()
```

All execution APIs support:

* per-call timeouts
* `raise_on_error`
* callbacks

## 🧠 Advanced features

### 🔌 Zero-Copy Bridge (Windows only)

> High-throughput shared-memory transfer between Python and PowerShell

Ideal for large binary blobs, files, or high-frequency data exchange.

```python
from virtualshell import Shell, ZeroCopyBridge

with Shell(timeout_seconds=60) as shell:
    with ZeroCopyBridge(shell) as bridge:
        data = b"x" * 1_000_000
        bridge.send(data, "$buf")
        print(shell.run("$buf.Length").out)
```

Typical throughput: **5–150 MB/s**
See the full guide in the wiki.

---

### 🧩 PowerShell object proxies

Generate Python `Protocol`s from PowerShell types and control live objects with IDE-friendly type hints.

```python
proxy = sh.make_proxy(
    "StreamWriterProxy",
    "System.IO.StreamWriter('test.txt')"
)

proxy.WriteLine("Hello")
proxy.Close()
```

📖 Docs:

* `generate_psobject`
* `make_proxy`

---

## 🧰 Core API overview

| Method                         | Description                     |
| ------------------------------ | ------------------------------- |
| `Shell.run(...)`               | Execute a command synchronously |
| `Shell.run_async(...)`         | Schedule async execution        |
| `Shell.script(...)`            | Run `.ps1` files                |
| `Shell.save_session()`         | Persist session snapshot        |
| `Shell.make_proxy(...)`        | Create live PS object proxy     |
| `Shell.generate_psobject(...)` | Generate Python `Protocol`s     |

---

## ⚙️ Configuration example

```python
Shell(
    powershell_path="C:/Program Files/PowerShell/7/pwsh.exe",
    working_directory="C:/automation",
    environment={"MY_FLAG": "1"},
    timeout_seconds=10,
    auto_restart_on_timeout=True,
    initial_commands=[
        "$ErrorActionPreference = 'Stop'",
        "$ProgressPreference = 'SilentlyContinue'",
    ],
)
```

---

## 📈 Performance

Latest benchmarks (Windows 11, Python 3.13):

* ~3.5 ms per sequential command
* ~3.2 ms per batch command
* ~2 ms async latency
* ~0.3 s session save

Full methodology and charts live in the wiki.

---

## 📚 Learn more

* 📖 [Documentation wiki](https://github.com/Chamoswor/virtualshell/wiki)
* 🐛 [Issues](https://github.com/Chamoswor/virtualshell/issues)
* 💬 [Discussions](https://github.com/Chamoswor/virtualshell/discussions)
