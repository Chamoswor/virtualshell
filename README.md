# virtualshell

High-performance PowerShell automation for Python. `virtualshell` keeps a single PowerShell host warm and exposes it through a thin Python wrapper backed by a C++ engine. The result: millisecond-scale latency, async execution, and session persistence without juggling subprocesses.

> Full documentation now lives in the [project wiki](https://github.com/Chamoswor/virtualshell/wiki). This README gives you the essentials and quick links.

---

## Why virtualshell?

- **Persistent session** – reuse modules, `$env:*`, and functions between calls.
- **Low latency** – avoid the 200+ ms penalty of `subprocess.run("pwsh")`; most commands settle in ~2-4 ms.
- **Async + batching** – schedule commands concurrently or in batches with strong timeout control.
- **Structured results** – every invocation returns stdout/stderr, exit code, success flag, and timing.
- **Predictable failures** – typed Python exceptions for “pwsh missing”, timeouts, and execution errors.
- **Type-safe automation** – generate Python `Protocol`s from PowerShell objects and create live proxies with full type hints.

Typical users embed PowerShell inside Python orchestration, long-running agents, or test suites that need reliability and speed.
---

## Installation

```bash
pip install virtualshell
```

Pre-built wheels are published for Windows, Linux (x86_64/aarch64), and macOS universal2. PowerShell (`pwsh` or `powershell.exe`) must be discoverable on `PATH` unless you pass an explicit path.

---

## Quick start

```python
from virtualshell import Shell

with Shell(timeout_seconds=5) as sh:
    result = sh.run("Write-Output 'Hello from pwsh'")
    print(result.out.strip())

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


### Scripts and arguments

```python
from pathlib import Path
from virtualshell import Shell

shell = Shell().start()

# Positional arguments
shell.script(Path("./scripts/test.ps1"), args=["alpha", "42"])

# Named arguments (hashtable splatting)
shell.script(
    Path("./scripts/test.ps1"),
    args={"Name": "Alice", "Count": "3"},
)

shell.stop()
```

Every API surface (sync/async/script) accepts `timeout` overrides and optional error raising via `raise_on_error` or callbacks.

---

## Zero-Copy Bridge (Windows only)

For high-throughput data transfer between Python and PowerShell, the Zero-Copy Bridge uses shared memory to eliminate serialization overhead. Ideal for large binary data, files, or high-frequency transfers.

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject

with Shell(timeout_seconds=60) as shell:
    with ZeroCopyBridge(shell) as bridge:
        # Python → PowerShell
        data = b"Large binary data" * 100000
        bridge.send(data, "$myData", timeout=30.0)
        res = shell.run("$myData.Length")
        print(f"PowerShell received {res.out.strip()} bytes")
    
        # PowerShell → Python
        shell.run("$response = [byte[]]::new(1048576)")
        data = bridge.receive("$response", timeout=30.0)
        print(f"Python received {len(data)} bytes")
```

For more complex scenarios involving PowerShell objects, you can serialize/deserialize them using the built-in methods:

```python
with Shell(timeout_seconds=60) as shell:
    # Get processes from PowerShell
    shell.run("$processes = Get-Process | Select-Object -First 5 -Property Name, Id, WorkingSet64")

    with ZeroCopyBridge(shell) as bridge:
        # Send the process list to Python
        bridge.serialize("processes", out_var="bytes")
        ps_bytes_raw: bytes = bridge.receive("bytes")

        # Convert bytes to PSObject in Python
        ps_obj = PSObject.from_bytes(ps_bytes_raw)

        # Print each process info
        for process in ps_obj["Items"]:
            print(f"Process Name: {process['Name']}, ID: {process['Id']}, Memory: {process['WorkingSet64']} bytes")

        # Modify the process names in Python
        for process in ps_obj["Items"]:
            process["Name"] = f"Modified_{process['Name']}"
        
        # Send modified object back to PowerShell
        bridge.send(ps_obj.to_bytes(), "processes")
        bridge.deserialize("processes")
        
        # Verify changes in PowerShell
        res = shell.run("$processes | Where-Object { $_.Name -like 'Modified_*' }")
        print("\nModified Processes in PowerShell:")
        print(res.out)

```

**Performance:** 5-150 MB/s depending on data size. Requires `win_pwsh.dll` (Windows only).

See [Zero-Copy Bridge guide](wiki/Usage/Zero-Copy%20Bridge.md) for complete documentation.

---

## PowerShell object proxies

`Shell.generate_psobject` reflects a PowerShell object into a Python `Protocol`, while `Shell.make_proxy` creates a live proxy that forwards attribute access back into PowerShell. Together they give you IDE-friendly, type-hinted automation. This is still an experimental feature and may not cover all edge cases.

```python
from virtualshell import Shell
from StreamWriter import StreamWriter  # generated via Shell().generate_psobject
from StreamReader import StreamReader

with Shell(strip_results=True, timeout_seconds=60) as sh:
    proxy_writer: StreamWriter = sh.make_proxy("StreamWriterProxy", "System.IO.StreamWriter('test.txt')") # type-hinted proxy (StreamWriter made via generate_psobject)

    proxy_writer.WriteLine("Test Line 1!")
    proxy_writer.WriteLine("Test Line 2!")
    proxy_writer.WriteLine("Test Line 3!")
    proxy_writer.Flush()
    proxy_writer.Close()

    proxy_reader: StreamReader = sh.make_proxy("StreamReaderProxy", "System.IO.StreamReader('test.txt')")

    while not proxy_reader.EndOfStream:
        print(f"Read: {proxy_reader.ReadLine()}")
```

Detailed guides live in the wiki: [generate_psobject](wiki/Usage/generate_psobject.md) and [make_proxy](wiki/Usage/make_proxy.md).

---

## Core API overview

| Method | Purpose |
| --- | --- |
| `Shell.run(cmd, *, timeout=None, raise_on_error=False)` | Execute a single command synchronously. |
| `Shell.run_async(cmd, *, callback=None, timeout=None)` | Schedule a command; returns a `concurrent.futures.Future`. |
| `Shell.script(path, args=None, *, timeout=None, dot_source=False, raise_on_error=False)` | Execute `.ps1` files with positional or named arguments. |
| `Shell.script_async(...)` | Async counterpart of `script`. |
| `Shell.save_session()` | Persist the current session to an XML snapshot. |
| `Shell.pwsh(text)` | Safely echo a literal PowerShell string (auto quoting). |
| `Shell.make_proxy(type_name, object_expression, *, depth=4)` | Create a live PowerShell object proxy. |
| `Shell.generate_psobject(type_expression, output_path)` | Generate a Python `Protocol` for a PowerShell object type. |

More helpers live in the wiki, including session restore, batching, and diagnostic tips.

---

## Configuration

```python
from virtualshell import Shell

shell = Shell(
    powershell_path="C:/Program Files/PowerShell/7/pwsh.exe",
    working_directory="C:/automation",
    environment={"MY_FLAG": "1"},
    timeout_seconds=10,
    auto_restart_on_timeout=True,
    stdin_buffer_size=64 * 1024,
    initial_commands=[
        "$ErrorActionPreference = 'Stop'",
        "$ProgressPreference = 'SilentlyContinue'",
    ],
    set_UTF8=True,
    strip_results=False,
)

shell.start()
```

Configuration is applied before the process starts. You can inspect later with `shell._core.get_config()`.

---

## Performance

Up-to-date benchmark artefacts (`bench.json`, `bench.csv`) and analysis live in [wiki/Project/Benchmarks.md](wiki/Project/Benchmarks.md). Headline numbers from the latest run (Windows 11, Python 3.13):

- Sequential commands: ~3.5 ms average
- Batch commands: ~3.2 ms per command
- Async latency: ~1.9–2.4 ms with 50 outstanding tasks
- Session save: ~0.30 s median

See the wiki for charts and methodology.

---

## Building from source

Dependencies:

- Python 3.8+
- CMake 3.20+
- A C++17 compiler (MSVC, Clang, or GCC)
- `scikit-build-core`, `pybind11`

```bash
python -m pip install -U build
python -m build
python -m pip install dist/virtualshell-*.whl
```

---

## Learn more

- [Usage guides](wiki/Usage)
- [Performance tips](wiki/Usage/Performance%20Tips.md)
- [Benchmarks](wiki/Project/Benchmarks.md)

Bug reports and feature requests are welcome via issues or discussions.

---

Licensed under the Apache 2.0 license. See [LICENSE](LICENSE).
