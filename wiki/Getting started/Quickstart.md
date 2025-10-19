### Page: Quickstart

```python
# Minimal single command
from virtualshell import Shell

with Shell() as sh:
    r = sh.run("Get-Date")
    print(r.success, r.out)
```

```python
# Batch of commands
from virtualshell import Shell

with Shell(timeout_seconds=5) as sh:
    results = sh.run(["$PSVersionTable.PSVersion", "Get-Random"])  # returns List[ExecutionResult]
    for i, r in enumerate(results):
        print(i, r.success, r.out)
```

```python
# Async single command with callback
from virtualshell import Shell

with Shell() as sh:
    fut = sh.run_async("Get-Date", callback=lambda res: print("done:", res.success))
    res = fut.result()  # wait
    print("value:", res.out)
```
