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

with Shell(timeout=30) as sh:       # default per-command timeout in seconds
    sh.run("Import-Module MyVendorModule")     # stays loaded
    r = sh.run("Get-Date")
    r.out          # stdout, stripped ("2" for "1+1")
    r.err          # stderr text
    r.success      # bool
    r.exit_code    # 0 ok, 1 command failed; negative = library-side:
                   # -1 timeout, -2 restarting, -3 host died
    r.execution_time
```

`Shell(timeout=...)` (default 5.0 s) is the session default; every execution
method also takes a per-call `timeout=`.

Default error behavior differs: `run()` returns failures as data
(`success=False`, text in `err`) unless you pass `raise_on_error=True`, while
`run_objects()` raises by default.

`run(list_of_commands)` executes a batch in order and returns a list of
results. `run_async(cmd)` returns a `concurrent.futures.Future`.
`script(path, args)` runs a `.ps1` with positional (list) or named (dict)
arguments. `Shell` is thread-safe, and blocking calls release the GIL, so
other Python threads keep running while PowerShell works — but one Shell is
one PowerShell process, which executes commands sequentially; use several
`Shell` instances for genuinely parallel PowerShell.

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

Values are normalized so results stay compact and identical on both editions:
dates come back as ISO-8601 strings (never `/Date(...)/`), enums as their
name, Guid/TimeSpan/Uri/Version as strings, FileInfo/DirectoryInfo as the
full path string, and any other rich .NET object at the `depth` boundary as
its `ToString()` instead of a property bag. Pass `raw=True` for plain
`ConvertTo-Json` output (full property bags, faster for huge dumps).

## Keep output out of your context window

```python
sh = Shell(max_output=4000)        # or per call: sh.run(cmd, max_output=4000)
res = sh.run("Get-ChildItem C:\\Windows -Recurse")
# res.out is now: head ... marker ... tail, where the marker looks like:
# ... [virtualshell: output truncated - 51203 chars / 1200 lines total,
#      showing first 2666 and last 1290 chars;
#      more: fetch_output('a1b2c3d4', offset=2666)] ...
res.truncated      # True when a budget was applied
res.output_key     # continuation key for res.out (None if not truncated)
res.error_key      # same for res.err

page = sh.fetch_output(res.output_key)            # OutputSlice
page.text, page.next_offset, page.total_chars, page.total_lines
```

Use the `output_key` field — no need to parse the marker text. Loop on
`page.next_offset` until it is `None`. The full text lives in a bounded
in-memory store (oldest evicted).

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
  `exit_code == -1` and the host restarts, reloading the newest
  `checkpoint()`/`save_session()` snapshot (no snapshot = state lost).
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
    dry_run_destructive=True,                               # -WhatIf dry runs
)
```

Rules, in evaluation order:

- `deny` always blocks. `confirm` matches need `on_confirm` to return truthy —
  **without an `on_confirm` handler, a `confirm` match raises
  `PolicyViolationError`**. A command approved via `on_confirm` runs for real
  (no dry run on top).
- `read_only=True` allows only a safe set: `Get-*`, `Find-*`, `Test-*`,
  `Measure-*`, `Select-*`, `Sort-*`, `Group-*`, `Compare-*`, `Format-*`,
  `ConvertTo-/ConvertFrom-*`, `Resolve-*`, `Where-Object`, `ForEach-Object`,
  `Write-*`, `Out-String/Null/Host/Default` and `Start-Sleep`. Everything
  else — including all `Set-*`, `New-*`, `Out-File` and external programs —
  is blocked. Extend the lane with `allow=["Import-Csv", ...]`.
- `dry_run_destructive=True` applies to its own `destructive=` pattern list
  (default: `Remove-*`, `Set-*`, `Stop-*`, `Clear-*`, `Disable-*`,
  `Uninstall-*`, `Restart-*`, `Reset-*`, and more), *not* to the `confirm`
  list. Matches run inside `& { $WhatIfPreference = $true; ... }`.

Commands are parsed (never executed) with PowerShell's own AST parser before
running; aliases resolve (`rm` -> `Remove-Item`), script-block bodies are
inspected recursively, and `script()` checks the file's content. Restrictive
policies block `& $var` / `Invoke-Expression` (not statically inspectable).

Limits — a policy is a guardrail, not a sandbox: `$x = 5` is an expression
(always passes); .NET method calls are not command invocations; and
`$WhatIfPreference` only affects cmdlets that implement ShouldProcess —
`[IO.File]::Delete(...)`, `cmd /c del` and other non-cmdlet paths run for
real even under `dry_run_destructive`.

## Don't hand scriptblocks to .NET events

Never register a PowerShell scriptblock as a .NET delegate or event handler
(`[System.ResolveEventHandler]{ ... }` casts, `$obj.add_Event({ ... })`):
when the delegate fires on a thread without a runspace it crashes the host
(StackOverflow) and loses all session state. virtualshell detects the pattern
and emits a `ScriptBlockDelegateWarning` (the command still runs). Compile
the handler in C# with `Add-Type` and attach that instead —
`Register-ObjectEvent` (engine-managed) is also safe.

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
`checkpoint()`/`save_session()` snapshot — and so does the automatic restart
after a timeout, so checkpointed state survives both. `restore(name)` /
`restore()` is manual undo for session state; `interrupt(restore=False)`
gives a clean host instead.

A checkpoint captures global variables, functions, aliases, loaded modules
(re-imported on restore — no need to `Import-Module` again), PSDrives,
environment variables and the current location. Snapshots are Clixml: live
.NET objects come back as property bags; recreate `make_proxy` proxies
afterwards.

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
from virtualshell import quote_pwsh_literal
sh.run(f"Write-Output {quote_pwsh_literal(user_text)}")
```

## Advanced (see wiki)

- `zero_copy_bridge()`: shared-memory bulk byte transfer (10-40x faster than
  stdout for MB-size payloads).
- `make_proxy` / `generate_psobject`: live typed proxies for .NET objects with
  generated `Protocol` stubs for IDE completion.
- Wiki: https://github.com/Chamoswor/virtualshell/wiki (start with
  "Agents & Guardrails").
