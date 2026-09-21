---
name: virtualshell
description: >
  Drive PowerShell from Python through one persistent, warm host (~2-4 ms per
  command). Use when a task needs PowerShell/.NET/Windows administration from
  Python or from an agent: persistent session state, structured results,
  objects instead of text, output budgets, execution policies, safe interrupt,
  and MCP-style tool schemas. Not for remote hosts (use pypsrp) and overkill
  for a single one-off command.
---

# Using virtualshell

`pip install virtualshell` (Python 3.11-3.14; needs `pwsh` on PATH, or
Windows PowerShell 5.1 on Windows). Verify: `python -c "import virtualshell"`.

## Mental model

One `Shell()` = one hidden, long-lived PowerShell process. Every `run()` goes
to the *same* session, so modules, variables, functions and auth context
persist between calls. The host runs with `-NonInteractive`: nothing can ever
prompt or hang waiting for input. You rarely need `start()` — every execution
method starts the backend on first use; `stop()` (or a `with` block) ends it.

```python
from virtualshell import Shell

with Shell() as sh:
    sh.run("Import-Module MyVendorModule")     # stays loaded
    r = sh.run("Get-Date")
    r.out          # stdout, stripped ("2" for "1+1")
    r.err          # stderr text
    r.success      # bool; r.exit_code (0 ok, -1 timeout, -3 host died)
    r.execution_time
```

`run(list_of_commands)` executes a batch in order and returns a list of
results. `run_async(cmd)` returns a `concurrent.futures.Future`.
`script(path, args)` runs a `.ps1` with positional (list) or named (dict)
arguments. `Shell` is thread-safe for concurrent `run` calls.

## Prefer objects over text

Never parse formatted PowerShell tables. Ask for objects:

```python
sh.run_objects("Get-Process", select=["Name", "Id"], first=5)
# [{'Name': 'chrome', 'Id': 1234}, ...]   - always a list
sh.run_objects("1..3")                     # [1, 2, 3]
sh.run_objects("@{a=@{b=1}}", depth=3)     # nested dicts (depth default 2)
```

Serialization happens in-session via ConvertTo-Json; assignments inside the
command persist. Terminating errors and stderr raise `ExecutionError` by
default; `raise_on_error=False` returns the objects that were produced anyway.
Use `select=`/`first=` to keep results small at the source.

## Keep output out of your context window

```python
sh = Shell(max_output=4000)        # or per call: sh.run(cmd, max_output=4000)
res = sh.run("Get-ChildItem C:\\Windows -Recurse")
# res.out is now: head ... marker ... tail, where the marker looks like:
# ... [virtualshell: output truncated - 51203 chars / 1200 lines total,
#      showing first 2666 and last 1290 chars;
#      more: fetch_output('a1b2c3d4', offset=2666)] ...
page = sh.fetch_output("a1b2c3d4", offset=2666)   # OutputSlice
page.text, page.next_offset, page.total_chars, page.total_lines
```

Loop on `page.next_offset` until it is `None`. The full text lives in a
bounded in-memory store (oldest evicted).

## Errors are typed - don't guess from strings

```python
from virtualshell import (VirtualShellError, ExecutionError,
                          ExecutionTimeoutError, PromptBlockedError,
                          PolicyViolationError, PowerShellNotFoundError)

sh.run(cmd, raise_on_error=True)   # or Shell(raise_on_error=True) session-wide
```

- `PromptBlockedError`: the command tried to prompt (`Read-Host`,
  `Get-Credential`, confirmation, missing mandatory parameter). It failed
  *immediately* — nothing is hanging. Re-run with the value as a parameter
  (`-Credential $cred`, `-Confirm:$false`).
- `ExecutionTimeoutError`: raised on timeout when
  `auto_restart_on_timeout=False`; with the default (True) a timeout returns
  `exit_code == -1` and the host restarts (state lost unless snapshotted).
- `PolicyViolationError` (`.command`, `.reason`, `.matched`): blocked by
  policy, never executed.
- Without `raise_on_error`, inspect `res.success` / `res.err`.

## Guardrails (ExecutionPolicy)

```python
from virtualshell import Shell, ExecutionPolicy

sh = Shell(policy=ExecutionPolicy(read_only=True))          # exploration lane
sh.policy = ExecutionPolicy(                                # swap anytime
    deny=["Stop-Computer", "Restart-Computer"],
    confirm=["Remove-*", "Stop-Process"],                   # needs approval
    on_confirm=lambda req: ask_human(req.command),          # req.matched too
    dry_run_destructive=True,                               # auto -WhatIf
)
```

Commands are parsed (never executed) with PowerShell's own AST parser before
running; aliases resolve (`rm` -> `Remove-Item`), script-block bodies are
inspected recursively, and `script()` checks the file's content. Restrictive
policies block `& $var` / `Invoke-Expression` (not statically inspectable).
Limits: `$x = 5` is an expression (always passes); .NET method calls are not
command invocations — a policy is a guardrail, not a sandbox.

## Long-running or runaway commands

```python
sh.run("$data = Import-Csv big.csv")   # expensive state
sh.checkpoint("loaded")                # named restore point (Clixml snapshot)

fut = sh.run_async("Invoke-SlowThing", timeout=300)
sh.interrupt()                         # aborts NOW (~0.5 s), not at timeout
fut.result().success                   # False
sh.run("$data.Count")                  # state restored from the checkpoint
```

`interrupt()` force-restarts the hidden host and reloads the newest
`checkpoint()`/`save_session()` snapshot; `restore(name)` / `restore()` is
manual undo for session state. Snapshots are Clixml: live .NET objects come
back as property bags; recreate `make_proxy` proxies afterwards.

## Learn an unfamiliar module before calling it

```python
sh.command_schema("Get-Process")            # one MCP-style tool definition
sh.module_schemas("VendorModule", timeout=120)  # every exported command
```

Returns `{"name", "description", "inputSchema": {properties, required}}` with
enums from `[ValidateSet]`/enum types, booleans for switches, and the .NET
type in `x-ps-type`. Use it to build correct calls on the first try instead of
probing.

## Two editions

`Shell()` prefers `pwsh` (PowerShell 7+) and falls back to Windows PowerShell
5.1 on Windows; force with `powershell_edition="core"|"desktop"`. Check
`sh.edition` before using 7-only syntax (ternary, `??`, `$PSStyle`).
Everything in this file works on both.

## Escaping user data

Never interpolate untrusted text into a command. Quote it:

```python
from virtualshell import Shell
from virtualshell.shell import quote_pwsh_literal
sh.run(f"Write-Output {quote_pwsh_literal(user_text)}")
```

## Advanced (see wiki)

- `zero_copy_bridge()`: shared-memory bulk byte transfer (10-40x faster than
  stdout for MB-size payloads).
- `make_proxy` / `generate_psobject`: live typed proxies for .NET objects with
  generated `Protocol` stubs for IDE completion.
- Wiki: https://github.com/Chamoswor/virtualshell/wiki (start with
  "Agents & Guardrails").
