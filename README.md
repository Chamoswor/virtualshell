# virtualshell

[![PyPI version](https://img.shields.io/pypi/v/virtualshell.svg)](https://pypi.org/project/virtualshell/)
[![Python versions](https://img.shields.io/pypi/pyversions/virtualshell.svg)](https://pypi.org/project/virtualshell/)
[![OS](https://img.shields.io/badge/OS-Windows%20%7C%20Linux%20%7C%20macOS-blue)](#)
[![License](https://img.shields.io/github/license/Chamoswor/virtualshell.svg)](LICENSE)
[![Build](https://img.shields.io/github/actions/workflow/status/Chamoswor/virtualshell/workflow.yml)](https://github.com/Chamoswor/virtualshell/actions)

**High-performance PowerShell automation for Python.**
Run PowerShell commands with millisecond latency, persistent sessions, async
execution, and a cross-platform zero-copy bridge — without juggling
subprocesses.

Full documentation lives in the
[wiki](https://github.com/Chamoswor/virtualshell/wiki). This README covers
what the library is, why it matters, and how to get started.

## What is virtualshell?

`virtualshell` keeps one PowerShell host warm behind a thin Python wrapper
powered by a C++ engine. Instead of spawning `pwsh` for every command, you
get:

* a persistent PowerShell session
* ~2–4 ms execution latency
* async and batch execution
* structured, predictable results
* optional zero-copy shared memory transfers (cross-platform)
* live object proxies with generated type stubs

Typical use cases include Python orchestration layers, long-running agents,
CI/test harnesses, and automation tooling that must be fast and reliable.

## Why use it?

* **Persistent session** — reuse loaded modules, `$env:*`, functions, and
  global state across calls.
* **Low latency** — avoid the ~200 ms cost of `subprocess.run("pwsh")` per
  command.
* **Async and batching** — schedule concurrent commands with timeouts and
  callbacks.
* **Structured results** — every call returns stdout, stderr, exit code,
  timing, and success state.
* **Predictable failures** — typed Python exceptions for timeouts, missing
  PowerShell, and execution errors.
* **Type-safe automation** — generate Python `Protocol` stubs from
  PowerShell objects and control them through live proxies.

## Requirements

* **Python 3.11 – 3.14**
* **PowerShell 7** (`pwsh`) on `PATH`, unless an explicit path is passed in
  the configuration

## Installation

Pre-built wheels are published for Windows (x64), Linux (x86_64 / aarch64),
and macOS (universal2):

```bash
pip install virtualshell
```

No compiler or build tools are required when installing wheels.

### Building from source (optional)

Building locally requires CMake and a C++17 compiler. On Windows, install
Visual Studio Build Tools with the *Desktop development with C++* workload
and run the build from an *x64 Native Tools Command Prompt* (64-bit only).

```bash
pip install virtualshell --no-binary virtualshell
```

Or directly from GitHub:

```bash
pip install "git+https://github.com/Chamoswor/virtualshell"
```

Verify the installation:

```bash
python -c "import virtualshell; print('virtualshell OK')"
```

## Quick start

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

All execution APIs support per-call timeouts, `raise_on_error`, and
callbacks.

## Advanced features

### Zero-copy bridge

High-throughput shared-memory transfer between Python and PowerShell, suited
for large binary blobs, files, or high-frequency data exchange. The payload
moves through a file-backed memory map shared between Python (`mmap`) and
PowerShell (.NET `MemoryMappedFile`) — no native DLL required, on Windows,
Linux, and macOS.

```python
from virtualshell import Shell, ZeroCopyBridge

with Shell(timeout_seconds=60) as shell:
    with ZeroCopyBridge(shell) as bridge:
        data = b"x" * 1_000_000
        bridge.send(data, "buf")
        print(shell.run("$buf.Length").out)
```

Measured throughput is roughly 145–560 MB/s depending on payload size and
direction — about 10–40× faster than piping the same data through stdout as
base64. See the wiki for the full guide, including `PSObject` round-trips
between Python and PowerShell.

### PowerShell object proxies

Control live .NET/PowerShell objects from Python. Scalars convert to native
Python types, byte arrays travel through the zero-copy bridge, and complex
values come back as sub-proxies.

```python
with Shell() as sh:
    writer = sh.make_proxy("", "System.IO.StreamWriter('test.txt')")
    writer.WriteLine("Hello")
    writer.Close()
```

Pair proxies with `generate_psobject`, which produces typed `Protocol` stubs
(including overloads) for IDE completion and static analysis:

```python
sh.generate_psobject("System.Text.StringBuilder()", Path("StringBuilder.py"))
```

```python
from StringBuilder import StringBuilder

sb = sh.make_proxy(StringBuilder)   # created and typed in one call
sb.Append("Hello")
```

See the `make_proxy` and `generate_psobject` pages in the wiki.

## Core API overview

| Method                         | Description                     |
| ------------------------------ | ------------------------------- |
| `Shell.run(...)`               | Execute a command synchronously |
| `Shell.run_async(...)`         | Schedule async execution        |
| `Shell.script(...)`            | Run `.ps1` files                |
| `Shell.save_session()`         | Persist a session snapshot      |
| `Shell.make_proxy(...)`        | Create a live PS object proxy   |
| `Shell.generate_psobject(...)` | Generate Python `Protocol`s     |
| `Shell.zero_copy_bridge()`     | Shared per-shell bridge         |

## Configuration example

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

## Performance

Measured with the bundled benchmarks (`bench/vs_bench.py`,
`bench/zcb_bench.py`) on Windows 11, Python 3.14, PowerShell 7:

**Command execution**

* ~0.45 s shell startup
* ~3.5–4.0 ms per sequential command (~280 cmd/s)
* ~3.2 ms per command in batches (~310 cmd/s)
* ~2.3 ms async latency
* ~620 cmd/s aggregate across 4 parallel shells
* ~0.35 s session save

**Zero-copy bridge** (best of 3 runs per size)

| Transfer                      | 1 MB     | 8 MB     | 32 MB    |
| ----------------------------- | -------- | -------- | -------- |
| PowerShell → Python (bytes)      | 145 MB/s | 385 MB/s | 413 MB/s |
| PowerShell → Python (memoryview) | 145 MB/s | 454 MB/s | 563 MB/s |
| Python → PowerShell (send)       | 178 MB/s | 369 MB/s | 293 MB/s |

A 4 KB send+receive round trip takes ~8 ms (p50). For reference, moving the
same 1 MB payload through stdout as base64 runs at ~14 MB/s, so the bridge
is roughly 10–40× faster for bulk data.

Full methodology and charts live in the wiki.

## Learn more

* [Documentation wiki](https://github.com/Chamoswor/virtualshell/wiki)
* [Issues](https://github.com/Chamoswor/virtualshell/issues)
* [Discussions](https://github.com/Chamoswor/virtualshell/discussions)
