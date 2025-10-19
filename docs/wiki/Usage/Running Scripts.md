### Page: Running Scripts

**Positional args**
```python
with Shell() as sh:
    res = sh.script("./scripts/do_stuff.ps1", args=["arg1", "arg2"])
```

**Named args**
```python
with Shell() as sh:
    res = sh.script("./scripts/do_stuff.ps1", args={"Name": "World", "Count": "5"})
```

**Dot‑sourcing** (mutates session state)
```python
with Shell() as sh:
    res = sh.script("./scripts/profile.ps1", dot_source=True)
```

**Async scripts**
```python
with Shell() as sh:
    fut = sh.script_async("./scripts/do_stuff.ps1", args={"Path": "."})
    print(fut.result().out)
```
