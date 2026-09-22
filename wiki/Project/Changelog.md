### Page: Changelog

```
## 1.4.0

API polish from agent field testing (TIA Portal automation):

- `Shell(timeout=...)` replaces `Shell(timeout_seconds=...)` — the session
  default now has the same name as the per-call `timeout=` argument.
  `timeout_seconds=` still works as a deprecated alias (DeprecationWarning)
- `run_objects` normalizes values before serialization: DateTime/
  DateTimeOffset -> ISO-8601 strings (no more PS 5.1 `/Date(...)/`), enums ->
  their name, Guid/TimeSpan/Uri/Version/IPAddress -> strings, FileInfo/
  DirectoryInfo -> the full path string; other rich .NET objects at the
  `depth` boundary become `ToString()` instead of a property bag. Output is
  now identical on both editions. `raw=True` restores plain ConvertTo-Json.
  Breaking: results that used to be nested property bags (e.g. FileInfo
  values) are now compact strings
- New `ScriptBlockDelegateWarning` (exported): emitted before executing a
  command that converts a PowerShell scriptblock into a .NET delegate/event
  handler (`[SomeEventHandler]{...}` casts, `$obj.add_Event({...})`), which
  can crash the host with a StackOverflow when the delegate fires on a
  thread without a runspace. The command still runs; `script()` file content
  is screened too

## 1.3.0

Agent-friendly execution surface (and a friendlier API for humans):

- `run(cmd, max_output=N)` bounds how much output a call returns: head+tail
  view with an inline continuation marker; the full text stays in a bounded
  in-memory store and `fetch_output(key, offset)` pages through it
  (`OutputSlice`). `Shell(max_output=...)` sets a session default
- `run_objects(cmd, select=..., first=..., depth=...)` returns the pipeline's
  objects as Python values (ConvertTo-Json in-session, parsed in Python,
  always a list); strict errors by default, assignments persist in the session
- `ExecutionPolicy` guardrails enforced before anything executes
  (`Shell(policy=...)` or `shell.policy = ...`): allow/deny glob patterns over
  the *resolved* command names (PowerShell AST parse in-session, recursive
  into script blocks, aliases resolved), `read_only=True` lanes,
  `confirm=[...]` + `on_confirm` approval hook, `dry_run_destructive=True`
  auto-WhatIf, `block_dynamic` for `& $var` / `Invoke-Expression`; blocked
  commands raise `PolicyViolationError` (`.command`, `.reason`, `.matched`);
  `script()` inspects the file's content the same way
- `interrupt()` aborts the running command *now* (fast force-restart of the
  host; pending futures fail immediately instead of waiting for their
  timeout) and reloads the newest checkpoint/save_session snapshot, so
  session state survives a runaway command
- `checkpoint(name)` / `restore(name_or_none)` / `checkpoints`: named restore
  points for session state on top of the existing snapshot scripts
- Commands that try to prompt (`Read-Host`, `Get-Credential`, confirmation
  prompts, missing mandatory parameters) raise the dedicated
  `PromptBlockedError` (subclass of `ExecutionError`) under
  `raise_on_error=True` instead of a generic error
- `command_schema(name)` / `module_schemas(module)` generate MCP-style tool
  definitions (JSON Schema parameters, enum values from ValidateSet/enum
  types, required from Mandatory, synopsis from Get-Help) so agents can call
  unfamiliar modules correctly on the first try
- Quality of life: `run()`/`script()`/... auto-start the backend (including
  after a host crash - no explicit `start()` needed); `strip_results` now
  defaults to True (`sh.run("1+1").out == "2"`); `Shell(raise_on_error=...)`
  sets a session-wide default for `run`/`script`
- Results from run() carry `truncated` / `output_key` / `error_key` fields,
  so paging a budget-truncated output needs no marker parsing
  (`ExecutionResult` is bound with dynamic attributes now)
- The blocking calls (execute/execute_batch/execute_script/execute_script_kv,
  start, stop) release the GIL: other Python threads keep running while
  PowerShell works (one Shell still executes commands sequentially - use
  several Shells for parallel PowerShell)
- checkpoint()/restore() keep the C++ session snapshot in sync, so the
  automatic restart after a timeout reloads the newest checkpoint too
  (previously only save_session() survived timeouts); interrupt(restore=False)
  removes the snapshot so the fresh host really starts clean
- `quote_pwsh_literal` is exported from the package root
- Breaking: results are stripped by default (pass `strip_results=False` for
  the raw stream text); `run()` after a dead host relaunches it instead of
  returning exit code -3

## 1.2.3
- Windows PowerShell 5.1 support with the full feature set (sync/async execution,
  scripts, session snapshots, zero-copy bridge, proxies, generate_psobject)
- New `powershell_edition` option ("auto" | "core" | "desktop") on `Shell` and the
  C++ `Config`; "auto" prefers pwsh and falls back to Windows PowerShell on Windows
- New `Shell.edition`, `Shell.powershell_version`, `Shell.powershell_path` and
  `Shell.configured_edition` properties; C++ `getPowerShellEdition()` /
  `getResolvedPowerShellPath()`
- `Config.powershell_path` now defaults to "" (resolved from the edition at start)
  instead of "pwsh"
- Integration tests run once per installed PowerShell edition
- Generated protocol stubs are self-sufficient: `generate_psobject` records the
  type's assembly (`__ps_assembly__` / `__ps_assembly_name__`) when it lives
  outside the .NET runtime, embeds `[Type]::new()` for `$variable` inputs whose
  type has a parameterless constructor, and takes `expression=` for the rest;
  `make_proxy(GeneratedClass)` loads the assembly (with a directory-probing
  `AssemblyResolve` handler, see `assembly_resolver.ps1`) before creating the
  object, so a stub works in a brand-new session
- `generate_psobject(..., follow=True)` writes a package of cross-annotated
  stubs for the whole SDK type graph reachable from the root (reflection-based,
  no live instances needed): properties, indexers, returns and parameters are
  typed with the generated classes, so completion works down the object graph
  (`tia.Projects.Item(0)` -> `Project`). `include_namespaces=` / `max_types=`
  bound the walk
- `PsProxy.generic(name, *type_args)(*args)` calls generic methods
  (`item.generic("GetService", SoftwareContainer)()`) in one round trip, with
  the closed MethodInfo cached per runtime type in the session
- `PsProxy.proxy_select(*expressions, **aliases)` reads members of every
  element of a collection in one round trip, for read-only listings
- Property reads and method calls now take one round trip instead of two
  (assignment and runtime-type read share a command)
- Static .NET members are reachable on instance proxies (`tia.GetProcesses()`,
  `dt.IsLeapYear(2024)`, `dt.UtcNow`), routed to `[Type]::Member`; both
  generators emit them, tagged `# static`
- Proxies of .NET collections implement the Python collection protocol:
  `len()`, indexing (negative indices, slices, keys), iteration and `in`,
  backed by Count/Length, the Item indexer, GetEnumerator and Keys/ContainsKey.
  Stubs annotate collections as `Sequence[T]` / `Mapping[K, V]` /
  `AbstractSet[T]` (not `List`/`Dict`/`Set`), `byte[]` as `bytes`
- Proxies record provenance (`PsProxy.ps_origin`: "$tia", "[T]::new(3)",
  "$tia.Projects", "$sb.Append('x')"); `generate_psobject` accepts a proxy (or
  its `ps_ref`) and embeds that path, so stubs for derived objects reach the
  object again instead of naming a temporary variable. `make_proxy` accepts
  derived "$var.Member" expressions as `obj_ref`
- A host process that dies on its own now fails in-flight commands immediately
  with exit code -3 ("PowerShell process exited unexpectedly", preceded by the
  host's last stderr) instead of waiting for the timeout; `start()` relaunches it
- Fixed a std::terminate (Python: "Fatal Python error: Aborted", exit code 3)
  when a Shell whose host had died was stopped or garbage-collected: the I/O
  pump skipped joining its reader threads once they had hit EOF

## 1.1.0
- Added per-command timeouts for async and batch commands
- New and improved wrapper api (implemented overloads for run* and script* methods)
- Internal improvements to command state management
- Ensured thread-safe restart handling, preventing race conditions during shell restarts
- Improved error handling and reporting in the C++ core
- Implemented optional debug logging in the C++ core for easier troubleshooting
- Updated documentation to reflect new features and changes
- Fixed various minor bugs and improved overall stability
- Enhanced test coverage for new features

## 1.0.2
- Wrapper fixes: improved error translation

## 1.0.1
- Initial public release with prebuilt wheels