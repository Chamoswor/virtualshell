### Page: Error Handling

**Timeouts**
```python
from virtualshell import ExecutionTimeoutError

with Shell(auto_restart_on_timeout=False) as sh:
    try:
        sh.run("Start-Sleep -Seconds 10", timeout=2, raise_on_error=True)
    except ExecutionTimeoutError as e:
        print("timed out:", e)
```

**General errors**
```python
from virtualshell import ExecutionError

with Shell() as sh:
    try:
        sh.run("throw 'boom'", raise_on_error=True)
    except ExecutionError as e:
        print("failed:", e)
```
