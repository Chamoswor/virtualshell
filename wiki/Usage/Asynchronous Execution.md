### Page: Asynchronous Execution

**Single**
```python
from concurrent.futures import TimeoutError

with Shell() as sh:
    fut = sh.run_async("Get-Date", callback=lambda res: print("cb:", res.success))
    res = fut.result(timeout=10)
```

**Batch with progress**
```python
# callback receives BatchProgress snapshots

def on_progress(p):
    print(f"{p.currentCommand}/{p.totalCommands}", "ok" if p.lastResult.success else "fail")

with Shell() as sh:
    fut = sh.run_async(["Get-Date", "Get-Random", "Get-Location"], callback=on_progress)
    all_results = fut.result()
```

**Notes**
- User callbacks are exception‑guarded to avoid breaking worker threads.