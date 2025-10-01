# virtualshell

High-performance Python façade over a **C++ PowerShell runner**.  
A single long-lived PowerShell process is managed in C++, handling pipes, threads, timeouts and output demux; Python exposes a small, predictable API.

---

## Features

- **Persistent session** to `pwsh`/`powershell` (reuse modules, `$env:*`, functions, cwd)
- **Sync & async** execution (Futures + optional callbacks)
- **Script execution** (positional / named args, optional dot-sourcing)
- **Batch** with per-command timeout & early-stop
- **Clear failures** (typed exceptions), **context manager** lifecycle

---

## Install

```bash
pip install virtualshell
````

## Platform & Python Support

Prebuilt wheels are provided via PyPI for common platforms and Python versions.  
This means you can usually `pip install virtualshell` without needing a compiler.

**Supported Python versions:**
- 3.8, 3.9, 3.10, 3.11, 3.12, 3.13

**Supported platforms:**
- **Windows** (x86_64, MSVC build)
- **Linux** (x86_64, aarch64, manylinux2014/2.28)
- **macOS** (universal2: x86_64 + arm64)

If your platform is not listed above, pip will fall back to building from source.  
See [Building from source](#building-from-source-advanced) for details.


> Requires PowerShell on `PATH` (`pwsh` preferred, `powershell` also supported).

---

## Quick start

```python
import virtualshell

# Create a shell with a 5s default timeout
sh = virtualshell.Shell(timeout_seconds=5).start()

# 1) One-liners (sync)
res = sh.run("Write-Output 'hello'")
print(res.out.strip())  # -> hello

# 2) Async single command
fut = sh.run_async("Write-Output 'async!'")
print(fut.result().out.strip())

# 3) Scripts with positional args
r = sh.run_script(r"C:\temp\demo.ps1", args=["alpha", "42"])
print(r.out)

# 4) Scripts with named args
r = sh.run_script_kv(r"C:\temp\demo.ps1", named_args={"Name":"Alice","Count":"3"})
print(r.out)

# 5) Context manager (auto-stop on exit)
with virtualshell.Shell(timeout_seconds=3) as s:
    print(s.run("Write-Output 'inside with'").out.strip())

sh.stop()
```

Another example (stateful session):

```python
from virtualshell import Shell
with Shell(timeout_seconds=3) as sh:
    sh.run("function Inc { $global:i++; $global:i }")
    nums = [sh.run("Inc").out.strip() for _ in range(5)]
    print(nums)  # ['1','2','3','4','5']
```

---

## API (overview)

```python
import virtualshell
from virtualshell import ExecutionResult  # dataclass view

sh = virtualshell.Shell(
    powershell_path=None,                     # optional explicit path
    working_directory=None,                   # resolved to absolute path
    timeout_seconds=5.0,                      # default per-command timeout
    environment={"FOO": "BAR"},               # extra child env vars
    initial_commands=["$ErrorActionPreference='Stop'"],  # post-start setup
).start()

# Sync
res: ExecutionResult = sh.run("Get-Location | Select-Object -Expand Path")

# Scripts
res = sh.run_script(r"/path/to/job.ps1", args=["--fast","1"])
res = sh.run_script_kv(r"/path/to/job.ps1", named_args={"Mode":"Fast","Count":"1"})
res = sh.run_script(r"/path/init.ps1", dot_source=True)

# Async
f = sh.run_async("Write-Output 'ping'")
f2 = sh.run_async_batch(["$PSVersionTable", "Get-Random"])

# Convenience
res = sh.pwsh("literal 'quoted' string")     # safe single-quoted literal

sh.stop()
```

### Return type

By default you get a Python dataclass:

```python
@dataclass(frozen=True)
class ExecutionResult:
    output: str
    error: str
    exit_code: int
    success: bool
    execution_time: float
```

Pass `as_dataclass=False` to receive the raw C++ result object.

### Timeouts

* Every method accepts a `timeout` (or `per_command_timeout`) in seconds.
* On timeout: `success=False`, `exit_code=-1`, `error` contains `"timeout"`.
* Async futures resolve with the timeout result; late output is dropped in C++.

---

## Design notes

* **Thin wrapper:** heavy I/O in C++; Python does orchestration only.
* **No surprises:** stable API, documented side-effects.
* **Clear failure modes:** `raise_on_error` and typed exceptions.
* **Thread-friendly:** async returns Futures/callbacks; no Python GIL-level locking.
* **Boundary hygiene:** explicit path/arg conversions, minimal marshalling.

### Security

* The wrapper **does not sanitize** raw commands. Only `pwsh()` applies literal single-quoting for data.
* Don’t pass untrusted strings to `run*` without proper quoting/sanitization.
* Avoid logging secrets; env injection happens via `Shell(..., environment=...)`.

### Performance

* Sync/async routes call into C++ directly; Python overhead is object creation + callback dispatch.
* Prefer **batch/async** for many small commands to amortize round-trips.

### Lifetime

* `Shell.start()` ensures a running backend; `Shell.stop()` tears it down.
* `with Shell(...)` guarantees stop-on-exit, even on exceptions.

---

## Exceptions

```python
from virtualshell.errors import (
    VirtualShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

try:
    res = sh.run("throw 'boom'", raise_on_error=True)
except ExecutionTimeoutError:
    ...
except ExecutionError as e:
    print("PowerShell failed:", e)
```

* `ExecutionTimeoutError` is raised on timeouts **if** `raise_on_error=True`.
* Otherwise, APIs return `ExecutionResult(success=False)`.

---

## Configuration tips

If PowerShell isn’t on `PATH`, pass `powershell_path`:

```python
Shell(powershell_path=r"C:\Program Files\PowerShell\7\pwsh.exe")
```

Session setup example:

```python
Shell(initial_commands=[
    "$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()",
    "$ErrorActionPreference = 'Stop'"
])
```

---

# Building from source (advanced)
Source builds require a C++ toolchain and CMake.

**Prereqs:** Python ≥3.8, C++17, CMake ≥3.20, `scikit-build-core`, `pybind11`.

```bash
# in repo root
python -m pip install -U pip build
python -m build           # -> dist/*.whl, dist/*.tar.gz
python -m pip install dist/virtualshell-*.whl
```

Editable install:

```bash
python -m pip install -e .
```

* Linux wheels target **manylinux_2_28** (x86_64/aarch64).
* macOS builds target **universal2** (x86_64 + arm64).

---

## Roadmap

* ✅ Windows x64 wheels (3.8–3.13)
* ✅ Linux x64/aarch64 wheels (manylinux_2_28)
* ✅ macOS x86_64/arm64 wheels
* ⏳ Streaming APIs and richer progress events

---

## License

Apache 2.0 — see [LICENSE](LICENSE).

---

*Issues & feedback are welcome. Please include Python version, OS, your PowerShell path (`pwsh`/`powershell`), and a minimal repro.*
