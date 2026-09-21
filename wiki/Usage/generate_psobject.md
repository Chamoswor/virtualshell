# generate_psobject

`generate_psobject` is a helper bundled with `virtualshell.Shell` that inspects a PowerShell object and produces a Python `Protocol` capturing its public surface. This lets IDEs and type-checkers offer autocompletion and static analysis when you interact with PowerShell objects from Python.

## Prerequisites

- Install the `virtualshell` package and ensure the native extension is built.
- A PowerShell host: `pwsh` (PowerShell 7+) on `PATH`, Windows PowerShell 5.1 on Windows, or an explicit `powershell_path` passed to `Shell`. Generation works on both editions; the reflected members naturally follow the .NET runtime of the host (.NET Framework 4.x under Windows PowerShell).
- The object you want to reflect must be constructible in the current PowerShell session (for example `System.Net.WebClient` or `[System.IO.StreamReader]::new('file.txt')`).

## Quick Start

```python
from virtualshell import Shell
from pathlib import Path

shell = Shell(strip_results=True, timeout_seconds=60)
shell.generate_psobject("System.Net.WebClient", Path("WebClient.py"))
```

Running this snippet writes `WebClient.py`, containing a protocol definition that mirrors the members reported by `Get-Member`. You can `import WebClient` in your project to benefit from completions and static typing.

## What the Helper Does

1. **Starts the shell if required:** When you call `generate_psobject`, the backing PowerShell host is started automatically if it is not already running. The shell will be stopped after generation if it was started implicitly.
2. **Normalises output:** The helper sets `$PSStyle.OutputRendering = 'PlainText'` and enables UTF-8 output so JSON parsing succeeds on any locale.
3. **Materialises the object:** The given expression is tried as-is, as a variable lookup, via `New-Object`, `[Type]::new()`, and `[Type]::New()` variants (including argument forwarding when you pass a call expression). It also attempts COM instantiation for qualified type names.
4. **Collects metadata:** Once an object is available, the helper executes `Get-Member -InputObject` (so collections report their own members), converts the results to JSON, and derives method/property metadata.
5. **Renders a protocol:** A minimal Python module is generated with imports, type annotations, and method/property signatures. Property types are derived from the member definitions, so `int Year {get;}` becomes `def Year(self) -> int`. Overloaded .NET methods are emitted as `@typing.overload` groups, so both `sb.Append("x")` and `sb.Append("x", 3)` type-check.

If all strategies fail to materialise an object, a `RuntimeError` is raised listing every attempted expression and its error.

## Customising Behaviour

- **Timeouts:** Pass a higher `timeout_seconds` when constructing the shell if the target object takes time to create.
- **PowerShell host:** Use `Shell(powershell_edition="core")` / `"desktop"` to pick pwsh or Windows PowerShell 5.1, or `Shell(powershell_path="C:/Program Files/PowerShell/7/pwsh.exe")` to reference a specific installation.
- **Result stripping:** `strip_results=True` trims trailing whitespace from PowerShell output; it is optional but helpful for clean JSON parsing.

## Rebuilding the Object in a New Session

The generated class carries everything `Shell.make_proxy(GeneratedClass)`
needs to recreate the object, so a stub generated once works in any later
session:

| Attribute | Meaning |
|-----------|---------|
| `__ps_type_name__` | The reflected type name. |
| `__ps_expression__` | The PowerShell expression that creates the object. For a `$variable` input this becomes `[Type]::new()` when the type has a parameterless constructor; otherwise the variable name is kept and `make_proxy` can only bind an existing object. Pass `expression=` to embed a real creation expression. |
| `__ps_assembly__`, `__ps_assembly_name__` | Present only for types outside the .NET runtime (vendor SDKs). `make_proxy` loads the assembly first and registers its directory with a process-wide `AssemblyResolve` probe so its dependencies resolve. |

**Derived objects.** Many SDK types have no constructor at all; you only get
them from a parent (`$tia.Projects`, `$project.Devices`, ...). Every proxy
remembers the expression it was reached by (`proxy.ps_origin`), and
`generate_psobject` accepts a proxy directly, so the stub records that path
instead of a throw-away variable name:

```python
tia = sh.make_proxy(TiaPortal)             # or sh.make_proxy("", "$tia")
projects = tia.Projects
print(projects.ps_origin)                  # -> "$tia.Projects"
sh.generate_psobject(projects, Path("ProjectComposition.py"))
# __ps_expression__ = '$tia.Projects'  (also when passing projects.ps_ref)
```

`make_proxy(ProjectComposition)` then works in any session where `$tia`
exists: the expression is evaluated once into a private variable. When the
root was *created* rather than bound (`make_proxy(TiaPortal)` with no
`obj_ref`), the recorded path starts from the creation expression, e.g.
`([Siemens.Engineering.TiaPortal]::new(...)).Projects`, which recreates the
root as well. Paths that involved bytes sent through the bridge or
out-buffers are not reproducible from source text; `ps_origin` is `None`
there and the stub falls back to the variable name.

```python
# Generate once, from a live object whose constructor needs arguments:
shell.generate_psobject(
    "$tia", Path("TiaPortal.py"),
    expression="[Siemens.Engineering.TiaPortal]::new("
               "[Siemens.Engineering.TiaPortalMode]::WithoutUserInterface)",
)

# Later, in a fresh session, no bootstrap needed:
from TiaPortal import TiaPortal
with Shell(powershell_edition="desktop") as sh:
    tia = sh.make_proxy(TiaPortal)   # loads Siemens.Engineering.Base.dll, then creates
```

## Generating a Typed Package (`follow=True`)

A single stub stops typing at object boundaries: `Projects` is `Any`, and
so is what `Item(0)` returns. With `follow=True` the generator walks the
**type graph** by reflection (no live instances needed, so an empty
collection's `Item(index)` is still typed) and writes a *package*: one
module per SDK type, each annotated with the others.

```python
shell.generate_psobject(
    "$tia", Path("tia_sdk"), follow=True,
    expression="[Siemens.Engineering.TiaPortal]::new("
               "[Siemens.Engineering.TiaPortalMode]::WithoutUserInterface)",
)
```

produces `tia_sdk/TiaPortal.py`, `tia_sdk/ProjectComposition.py`,
`tia_sdk/Project.py`, ... and an `__init__.py` re-exporting them:

```python
class TiaPortal(Protocol):
    @property
    def Projects(self) -> ProjectComposition: ...
    def GetCurrentProcess(self) -> TiaPortalProcess: ...

class ProjectComposition(Protocol):
    def Item(self, index: int) -> Project: ...
    def Open(self, path: Any) -> Project: ...      # System.IO.FileInfo: runtime type
```

```python
from tia_sdk import TiaPortal

tia = sh.make_proxy(TiaPortal)
project = tia.Projects.Item(0)      # typed Project, completion all the way down
```

Rules of the walk:

- Only types outside the .NET runtime are followed (the SDK). Runtime types
  map to Python scalars (`str`, `int`, `datetime.datetime`, ...) or `Any`;
  enums map to `str`, which is what proxies return for them.
- Generic collections map like the single-file generator: `IEnumerable<T>`
  / `List<T>` become `List[T]`, dictionaries `Dict[K, V]`, `Nullable<T>`
  `Optional[T]`, with `T` resolved to the generated class when followed.
- Indexers (`this[int]`) become methods (`Item(index)`), overloads become
  `@overload` groups; cross-module references are `TYPE_CHECKING` imports,
  so importing the package never creates cycles at runtime.
- `include_namespaces=["Siemens.Engineering"]` limits following to those
  namespaces and the namespaces nested under them (`Siemens.Engineering.HW`,
  `.SW`, ...); with an empty list every type outside the runtime is
  followed. Useful when a vendor SDK drags in types you do not want stubs
  for. `max_types` (default 2000) caps the walk.
- Only the root module gets `__ps_expression__` (from `expression=`, the
  proxy's `ps_origin`, or `[Type]::new()`). Other types are reached through
  a parent; `make_proxy(Project)` without `obj_ref` raises `TypeError`
  saying so. The root may also be a bare type literal
  (`"[Siemens.Engineering.TiaPortal]"`) when no instance exists yet.
- Every module carries `__ps_assembly__`, so any stub loads its SDK on use.

## Using the Generated Protocol

After generation, import the new module and annotate your variables:

```python
from WebClient import WebClient
from virtualshell import Shell

shell = Shell().start()

client = shell.make_proxy(WebClient)  # created and typed in one call
print(client.BaseAddress)

shell.stop()
```

Generated classes embed ``__ps_type_name__`` and ``__ps_expression__``
metadata, which is what lets `make_proxy(WebClient)` recreate the object
without repeating the PowerShell expression. Pass an explicit variable to
bind instead: `shell.make_proxy(WebClient, "$client")`.

## Static Classes

Passing a bare `[Type]` literal generates a protocol for the type's
**static** surface instead (collected with `Get-Member -Static`):

```python
shell.generate_psobject("[System.Math]", Path("Math.py"))
```

The generated class additionally embeds ``__ps_static__ = True``, so
`make_proxy(Math)` binds a [static proxy](make_proxy.md#static-classes)
automatically:

```python
from Math import Math

math = shell.make_proxy(Math)
math.Sqrt(16.0)     # 4.0 - with full IDE completion
```

The protocol only describes the members discovered at generation time. If the PowerShell type changes or you need a different view, rerun `generate_psobject` with the updated command.

### End-to-End Example

Once you have generated a protocol (for example `WebClient.py`), you can attach it to a live proxy and keep full type information while calling into PowerShell:

```python
from virtualshell import Shell
from WebClient import WebClient

with Shell(strip_results=True, timeout_seconds=60) as sh:
	sh.run("$client = New-Object System.Net.WebClient") # create the object in PS
	client: WebClient = sh.make_proxy("WebClientProxy", "$client") # type-hinted proxy

	url = "https://www.example.com"
	data = client.DownloadString(url)

	print(f"Downloaded data from {url}:\n{data[:100]}...")
```

See the [`make_proxy`](make_proxy.md) guide for full details on the proxy API and advanced options.

## Troubleshooting

- **Object creation fails:** Ensure the expression you pass works in PowerShell. For complex constructors, use PowerShell syntax (e.g. `[System.IO.StreamReader]::new('file.txt')`).
- **Missing members:** `Get-Member` might exclude hidden members by default. Adjust your expression or extend the helper if you need additional metadata.
- **Encoding issues:** Confirm that UTF-8 is set in the PowerShell session (the helper sets this automatically).

By combining PowerShell introspection with Python protocols, `generate_psobject` offers a quick path to ergonomic, type-aware automation scripts.
