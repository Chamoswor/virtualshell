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

**Host process crashes**

If the PowerShell process dies on its own (for example a .NET `StackOverflowException` inside a command, or someone killing the process), virtualshell notices the broken pipes immediately:

- every command that was in flight completes at once with `exit_code == ExitCode.NOT_RUNNING` (-3) and an `err` that ends with `PowerShell process exited unexpectedly.`, preceded by whatever the host printed to stderr before dying, so the command does not wait for its timeout;
- `is_running` becomes `False`;
- the next `run()` (or any other execution method) relaunches the host automatically — `auto_restart_on_timeout` does not apply, since nothing timed out.

```python
res = sh.run("[Environment]::FailFast('boom')")
assert res.exit_code == ExitCode.NOT_RUNNING
sh.run("1+1")       # auto-relaunch; session state is gone unless a
                    # save_session()/checkpoint() snapshot exists
```

**Blocked prompts**

The host runs with `-NonInteractive`: `Read-Host`, `Get-Credential`,
confirmation prompts and missing mandatory parameters fail immediately instead
of hanging. With `raise_on_error=True` these failures raise the dedicated
`PromptBlockedError` (a subclass of `ExecutionError`):

```python
from virtualshell import PromptBlockedError

try:
    sh.run("Read-Host 'value'", raise_on_error=True)
except PromptBlockedError:
    ...  # pass the value as a parameter instead
```

**Policy violations**

When the Shell has an `ExecutionPolicy`, blocked commands raise
`PolicyViolationError` *before* anything executes; see
[Agents & Guardrails](Agents-&-Guardrails).
