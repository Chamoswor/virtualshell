### Page: Agents & Guardrails

virtualshell 1.3 adds a surface designed for LLM agents and automation
harnesses — and it makes the library nicer for humans too. The four problems
it solves:

1. **Token cost** — an unfiltered `Get-Process` can eat tens of thousands of
   tokens. → output budgets (`max_output`) and structured results
   (`run_objects`).
2. **Uncertainty** — free-text errors, prompts that look like hangs. → typed
   `PromptBlockedError`, immediate failures instead of hangs.
3. **Safety** — an agent typing `Remove-Item -Recurse` unsupervised. →
   `ExecutionPolicy` with allow/deny lists, read-only lanes, confirmation
   hooks and automatic `-WhatIf` dry runs.
4. **Recovery** — a runaway command should not cost the session. →
   `interrupt()` plus `checkpoint()` / `restore()`.

Everything below works on both PowerShell editions (`pwsh` and Windows
PowerShell 5.1).

---

#### Output budgets: `max_output` + `fetch_output`

```python
from virtualshell import Shell

sh = Shell(max_output=4000)            # session default; per-call override too

res = sh.run("Get-ChildItem C:\\Windows\\System32")   # huge
print(res.out)
# <first ~2600 chars>
# ... [virtualshell: output truncated - 51203 chars / 1200 lines total,
#      showing first 2666 and last 1290 chars;
#      more: fetch_output('a1b2c3d4', offset=2666)] ...
# <last ~1300 chars>
```

The *full* output stays in memory (bounded store, oldest evicted). The result
carries the continuation key as a field — no need to parse the marker:

```python
res.truncated      # True when a budget was applied
res.output_key     # continuation key for res.out (None if not truncated)
res.error_key      # same for res.err

page = sh.fetch_output(res.output_key)
page.text          # next chunk
page.next_offset   # pass back in; None when done
page.total_chars, page.total_lines
```

`max_output=0` (or omitting it) means unlimited. The budget also applies to
`err`.

---

#### Objects instead of text: `run_objects`

```python
sh.run_objects("Get-Process", select=["Name", "Id"], first=5)
# [{'Name': 'chrome', 'Id': 1234}, ...]

sh.run_objects("Get-Service", select="Status", first=3)
sh.run_objects("1..10", first=3)          # [1, 2, 3]
sh.run_objects("@{a=@{b=1}}", depth=3)    # nested dicts
```

- Serialization happens in the session (`ConvertTo-Json`), parsing in Python —
  no screen scraping, and always a list.
- `select=` / `first=` keep results small at the source.
- Runs in the current scope: assignments inside the command persist.
- Errors are strict by default: terminating errors and stderr raise
  `ExecutionError` (`raise_on_error=False` returns whatever objects were
  produced anyway).

---

#### Policy: allow/deny, read-only lanes, confirmation, dry runs

```python
from virtualshell import Shell, ExecutionPolicy, PolicyViolationError

def ask_human(req):
    # req.command, req.names, req.matched
    return input(f"Allow {req.command!r}? [y/N] ").lower() == "y"

sh = Shell(policy=ExecutionPolicy(
    deny=["Stop-Computer", "Restart-Computer", "Format-Volume"],
    confirm=["Remove-*", "Stop-Process"],
    on_confirm=ask_human,
))

sh.run("Stop-Computer")        # raises PolicyViolationError, never executes
```

A read-only lane for exploration:

```python
sh.policy = ExecutionPolicy(read_only=True)
sh.run("Get-Process | Select-Object -First 3")     # fine
sh.run("Remove-Item x")                            # PolicyViolationError
sh.run("rm x")                                     # also blocked - aliases resolve
sh.run("1..3 | ForEach-Object { Stop-Process $_ }")# blocked - nested blocks are seen
sh.run("& $cmd")                                   # blocked - not statically inspectable
```

Automatic `-WhatIf` for destructive verbs:

```python
sh.policy = ExecutionPolicy(dry_run_destructive=True)
sh.run("Remove-Item big.log")   # runs as a dry run, prints "What if: ...",
                                # deletes nothing
```

`dry_run_destructive` matches its own `destructive=` list (default:
`Remove-*`, `Set-*`, `Stop-*`, `Clear-*`, `Disable-*`, `Uninstall-*`,
`Restart-*`, `Reset-*`, ...), not the `confirm` list. Evaluation order is
deny → dynamic → read-only/allow → confirm → dry run; a command approved via
`on_confirm` runs for real, and a `confirm` match **without** an `on_confirm`
handler is blocked. The dry run sets `$WhatIfPreference`, which only affects
cmdlets that implement ShouldProcess — `[IO.File]::Delete(...)` or
`cmd /c del` are untouched by it (see the limits below).

How it works: before anything executes, the command is parsed with
PowerShell's own AST parser in the hosted session (parse only — nothing runs),
every `CommandAst` is collected recursively, aliases are resolved, and the
names are matched (case-insensitive globs) against the policy. Scripts run via
`shell.script(...)` get their file content inspected the same way. Blocked
commands raise `PolicyViolationError` (with `.command`, `.reason`,
`.matched`); for a batch, validation happens before *anything* executes.

Honest limits — a policy is a guardrail, not a sandbox:

- `$x = 5` is an expression, not a command; session state can always change.
- .NET calls (`[IO.File]::Delete(...)`) are not command invocations, and
  `$WhatIfPreference` does not affect them (nor external programs) — only
  cmdlets that implement ShouldProcess honor the dry run.
- `& $var` / `Invoke-Expression` cannot be inspected statically; restrictive
  policies (read-only or allow-list) block them, lenient ones let them pass
  (set `block_dynamic=True` to block them always).

The read-only lane allows `Get-*`, `Find-*`, `Search-*`, `Test-*`,
`Measure-*`, `Select-*`, `Sort-*`, `Group-*`, `Compare-*`, `Format-*`,
`ConvertTo-/ConvertFrom-*`, `Resolve-*`, `Where-Object`, `ForEach-Object`,
`Write-*`, `Join-Path`/`Split-Path`/`Join-String`, `Start-Sleep`, and the
non-writing `Out-` cmdlets (`Out-String`, `Out-Null`, `Out-Host`,
`Out-Default` — **not** `Out-File`). Everything else, including every `Set-*`
and `New-*` and any external program, is blocked; extend with `allow=`.

---

#### Prompts fail fast, typed

The host runs with `-NonInteractive`, so `Read-Host`, `Get-Credential`,
confirmation prompts and missing mandatory parameters can never hang waiting
for input that will not come — PowerShell fails them immediately, and
virtualshell raises a dedicated type when you asked for exceptions:

```python
from virtualshell import PromptBlockedError

try:
    sh.run("Read-Host 'password'", raise_on_error=True)
except PromptBlockedError:
    ...  # re-run with the value passed as a parameter
```

Without `raise_on_error`, the result simply has `success == False` and the
host's message in `err`.

---

#### Cancel without losing state: `interrupt` + `checkpoint`

The stdin-based host cannot stop a running pipeline in place, so `interrupt()`
force-restarts the process (fast) and reloads the newest snapshot:

```python
sh.run("$data = Import-Csv big.csv")   # expensive state
sh.checkpoint("loaded")                 # named restore point

fut = sh.run_async("Invoke-SlowVendorCall")   # runaway
sh.interrupt()                          # returns in ~0.5 s, not at the timeout
fut.result().success                    # False - aborted immediately

sh.run("$data.Count")                   # state survived via the checkpoint
```

- Pending commands fail immediately (`success=False`) instead of waiting for
  their timeout.
- The automatic restart after a *timeout* reloads the same newest snapshot,
  so checkpointed state survives timeouts exactly like interrupts.
- `restore=False` starts a clean host instead (named checkpoints remain
  restorable by hand).
- `checkpoint(name)` / `restore(name)` also work standalone as undo for
  session state — `restore()` with no argument reloads the newest snapshot.
- A checkpoint captures global variables, functions, aliases, loaded modules
  (re-imported on restore), PSDrives, environment variables and the current
  location.
- Snapshots are Clixml: live .NET objects come back as property bags, and
  `make_proxy` proxies / the zero-copy bridge must be recreated after an
  interrupt.

---

#### Tool schemas from `Get-Command`

Generate MCP-style tool definitions so an agent can call an unfamiliar module
correctly on the first try:

```python
sh.command_schema("Get-Process")
# {'name': 'Get-Process',
#  'description': 'Gets the processes that are running...',
#  'inputSchema': {'type': 'object',
#                  'properties': {'Name': {'type': 'array', ...}, ...},
#                  'required': []},
#  'x-ps-module': 'Microsoft.PowerShell.Management', ...}

sh.module_schemas("MyVendorModule", timeout=120)   # every exported command
```

`[ValidateSet]` and enum parameter types become `"enum"`, `Mandatory`
parameters become `"required"`, switches become booleans, and the original
.NET type is kept in `"x-ps-type"`. `include_help=False` skips the Get-Help
synopsis lookup when speed matters.
