### Page: Synchronous Execution

**Single command**
```python
with Shell()as sh:
    r = sh.run("Get-Process | Select-Object -First 1 Name")
    assert r.success
```

**Batch**
```python
with Shell() as sh:
    rs = sh.run(["Get-Date", "Get-Random", "Get-Location"])  # List[ExecutionResult]
    for r in rs:
        if not r.success:
            print("failed:", r.err)
```

**Raising on error**
```python
with Shell() as sh:
    r = sh.run("throw 'boom'", raise_on_error=False)  # no Python exception
    try:
        sh.run("throw 'boom'", raise_on_error=True)   # raises ExecutionError
    except Exception as e:
        print(e)
```
