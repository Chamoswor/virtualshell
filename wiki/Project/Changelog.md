### Page: Changelog

```
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