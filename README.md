# virtualshell

High-performance Python façade over a **C++ PowerShell runner**.
All heavy I/O (pipes, threads, timeouts, demux) is implemented in C++ for low latency and high throughput; Python exposes a thin API.

---

## Features

* **Single persistent session** to `pwsh`/`powershell` (low overhead)
* **Sync & async** execution (futures + optional callbacks)
* **Script execution** with positional or named args
* **Batch execution** with per-command timeout & early-stop
* **Clean error model** with typed Python exceptions
* **Context manager** (`with Shell(...)`) for lifecycle safety

---

## Install

```bash
pip install virtualshell==0.1.2
```

### Available distributions (Windows/Linux, amd64/x86_64)


> **Requirements**
>
> * Windows 10/11 x64 | Linux x86_64
> * PowerShell available as `pwsh` (preferred) or `powershell` on `PATH`
> * Python 3.8–3.13 (matching the wheel you install)

---

## Quick start

```python
import virtualshell

# Create a shell with a 5s default timeout
sh = virtualshell.Shell(timeout_seconds=5).start()

# 1) One-liners (sync)
res = sh.execute("Write-Output 'hello'")
print(res.output.strip())  # -> hello


# 3) Async single command
fut = sh.execute_async("Write-Output 'async!'")
print(fut.result().output.strip())

# 4) Scripts with positional args
r = sh.execute_script(r"C:\temp\demo.ps1", args=["alpha", "42"])
print(r.output)

# 5) Scripts with named args
r = sh.execute_script_kv(r"C:\temp\demo.ps1", named_args={"Name":"Alice", "Count":"3"})
print(r.output)

# 6) Context manager (auto-stop on exit)
with virtualshell.Shell(timeout_seconds=3) as s:
    print(s.execute("Write-Output 'inside with'").output.strip())

sh.stop()
```

```python
from virtualshell import Shell
with Shell(timeout_seconds=3) as sh:
    sh.execute("function Inc { $global:i++; $global:i }")
    nums = [sh.execute("Inc").output.strip() for _ in range(5)]
    print(nums)  # -> ['1', '2', '3', '4', '5']

```

---

## Python API (high-level façade)

```python
import virtualshell
from virtualshell import ExecutionResult  # optional: Python dataclass view

sh = virtualshell.Shell(
    powershell_path=None,             # optional explicit path to pwsh/powershell
    working_directory=None,           # resolved to absolute path if provided
    timeout_seconds=5.0,              # default per-command timeout
    environment={"FOO":"BAR"},        # extra env vars for the child process
    initial_commands=["$ErrorActionPreference='Stop'"]  # run after start()
)

sh.start()                            # idempotent, safe if already running

# --- Sync ---
res: ExecutionResult = sh.execute("Get-Location | Select-Object -Expand Path")
print(res.success, res.exit_code, res.output)

res = sh.execute_script(r"C:\scripts\job.ps1", args=["--fast","1"])
res = sh.execute_script_kv(r"C:\scripts\job.ps1", named_args={"Mode":"Fast","Count":"1"})

# Dotsource script
res = sh.execute_script(r"C:\scripts\init.ps1", dot_source=True)

# --- Async ---
f = sh.execute_async("Write-Output 'ping'")
print(f.result().output.strip())

def on_done(r: ExecutionResult) -> None:
    print("DONE:", r.success, r.output.strip())

sh.execute_async("Write-Output 'callback!'", callback=on_done)

# Async batch
f2 = sh.execute_async_batch(["$PSVersionTable", "Get-Random"])
print([r.success for r in f2.result()])

# --- Convenience ---
res = sh.pwsh("literal 'quoted' string")  # safely single-quote literal data

sh.stop()
```

### Return types

By default, methods return a Python `ExecutionResult` dataclass:

```python
@dataclass(frozen=True)
class ExecutionResult:
    output: str
    error: str
    exit_code: int
    success: bool
    execution_time: float
```

Pass `as_dataclass=False` to receive the **raw C++ result object** for zero-copy scenarios.

### Timeouts

* Each API accepts a `timeout` (or `per_command_timeout`) in **seconds**.
* On timeout, `success=False`, `exit_code=-1`, `error` contains `"timeout"`.
* Async futures resolve with the timeout result; late output is discarded by the C++ layer.

---

## Design goals (production-readiness)

* **Thin wrapper:** All heavy I/O and process orchestration live in C++ for performance.
* **No surprises:** Stable API; no implicit state mutations beyond what is documented.
* **Clear failure modes:** Dedicated exceptions and `raise_on_error` semantics.
* **Thread-friendly:** Async methods return Futures and accept callbacks; no Python-side locks.
* **Boundary hygiene:** Minimal data marshalling; explicit conversions for paths/args.

### Security notes

* The wrapper **does not sanitize** raw commands. Only `pwsh()` uses literal single-quoting to protect data as arguments.
* Do **not** pass untrusted strings to `execute*` without proper quoting/sanitization.
* Environment injection happens via the `Shell` config; avoid secrets in logs/tracebacks.

### Performance notes

* Sync/async routes call into C++ directly; Python overhead is mostly object allocation and callback dispatch.
* Prefer **batch** or **async** when issuing many small commands to amortize round-trips.

### Lifetime

* `Shell.start()` initializes/ensures a running backend; `Shell.stop()` tears it down.
* `with Shell(...) as sh:` guarantees **stop-on-exit**, even on exceptions.

### Compatibility

* The C++ layer may expose both `snake_case` and `camelCase`.
* `ExecutionResult.from_cpp()` normalizes fields to keep ABI compatibility.

---

## Exceptions

```python
from virtualshell.errors import (
    SmartShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

try:
    res = sh.execute("throw 'boom'", raise_on_error=True)
except ExecutionTimeoutError:
    ...
except ExecutionError as e:
    print("PS failed:", e)
```

* `ExecutionTimeoutError` is raised when `exit_code == -1` and the error mentions `timeout`, **if** `raise_on_error=True`.
* Otherwise APIs return an `ExecutionResult` with `success=False`.

---

## Configuration tips

* If `pwsh`/`powershell` isn’t on `PATH`, pass `powershell_path` to `Shell(...)`.
* Use `initial_commands` for per-session setup, e.g. UTF-8:

```python
Shell(initial_commands=[
    "$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()",
    "$ErrorActionPreference = 'Stop'"
])
```

---

## Building from source (advanced)

You typically don’t need this when using wheels, but if you want to build locally:

### Prerequisites

* **Python** ≥ 3.8 with dev headers
* **C++17** compiler
* **CMake** ≥ 3.20
* **Build backend:** [`scikit-build-core`](https://github.com/scikit-build/scikit-build-core) + [`pybind11`](https://pybind11.readthedocs.io/)
* **Windows:** MSVC (VS 2019/2022)
* **Linux:** GCC/Clang (Linux wheels not verified yet)

The project is already configured via **`pyproject.toml`** and **`CMakeLists.txt`**. The compiled extension is installed as **`virtualshell._core`**, so `import virtualshell` works out of the box.

### One-shot local build (recommended)

```bash
# in repo root
python -m pip install -U pip build
python -m build  # produces sdist and wheel under ./dist
python -m pip install dist/virtualshell-*.whl
```

### Editable/dev install

```bash
python -m pip install -U pip
python -m pip install -e .  # uses scikit-build-core to build the C++ extension
```

### Platform notes

**Windows (x64)**

* Visual Studio 2022 generator is used by default (see `[tool.scikit-build.cmake]` in `pyproject.toml`).
* If you have multiple VS versions, ensure the correct **x64** toolchain is active (Developer Command Prompt or `vcvars64.bat`).

**Linux (x86_64)**

* Source builds work with a recent GCC/Clang + CMake.
* Prebuilt manylinux wheels are **not** published yet; CI configuration exists, but the Linux runtime matrix is still being validated.

### Build configuration

Most options are declared in `pyproject.toml`:

* **Backend:** `scikit_build_core.build`
* **Build args:** CMake generator and `PYBIND11_FINDPYTHON=ON` are set (auto-discovers the active Python).
* **Wheel layout:** packaged under `src/virtualshell/`
* **Versioning:** `setuptools_scm` writes `src/virtualshell/_version.py` from Git tags.

You can override or pass extra CMake definitions at build time if needed:

```bash
# Example: switch generator or tweak parallelism
SCIKIT_BUILD_VERBOSE=1 \
CMAKE_BUILD_PARALLEL_LEVEL=8 \
python -m build
```

### Smoke test after build

```bash
python - << 'PY'
import virtualshell
s = virtualshell.Shell(timeout_seconds=2)
print("import_ok:", bool(s._core))
# Optional: only if PowerShell is available on PATH
# if s.start().is_running:
#     print("exec_ok:", virtualshell.Shell().start().execute("Write-Output 'ok'").success)
PY
```

### Troubleshooting

* **Cannot find MSVC/CMake:** open a *Developer Command Prompt for VS 2022* or ensure `cmake` and the MSVC toolchain are on `PATH`.
* **ImportError: cannot import name `_core`:** the extension didn’t build or wasn’t placed under `virtualshell/_core.*`. Reinstall (`python -m pip install -e .` or `python -m build && pip install dist/*.whl`).
* **PowerShell not found at runtime:** pass an explicit path: `Shell(powershell_path=r"C:\Program Files\PowerShell\7\pwsh.exe")`.

---

## Roadmap

* ✅ Windows x64 wheels (3.8–3.13)
* ✅ Linux x64 wheels (manylinux)
* ⏳ Streaming APIs and richer progress events
* ✅ Packaging polish (`pyproject`, build matrices, GitHub Actions)

---

## License

Apache 2.0, see [LICENSE](LICENSE) for details.

---

## Acknowledgments

* Built with `pybind11`, and a lot of care around cross-platform pipes & process control.

---

*If you hit issues, please open an issue with your Python version, OS, `pwsh`/`powershell` path, and a minimal repro.*
