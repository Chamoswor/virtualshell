# make_proxy

`Shell.make_proxy` returns a live Python proxy for a PowerShell/.NET object. Attribute reads, property writes and method calls run inside the PowerShell session, and results are converted to Python values based on their **actual runtime type** — never guessed from metadata.

The proxy is implemented in pure Python and works on Windows, Linux and macOS. Bulk binary data (byte arrays, buffers) automatically travels through the [Zero-Copy Bridge](Zero-Copy%20Bridge.md) instead of the text channel.

## Signature

```python
proxy = shell.make_proxy(type_name: str, obj_ref: str | None = None, *, static: bool = False)
proxy = shell.make_proxy(GeneratedProtocol, obj_ref: str | None = None, *, static: bool = False)
```

| Parameter | Description |
|-----------|-------------|
| `type_name` | Optional label (`""` is fine — the real type is resolved from the live object), **or** a Protocol class produced by `generate_psobject`. |
| `obj_ref` | Either an existing PowerShell variable (`"$client"`), or a **creation expression** such as `"System.Text.StringBuilder(32)"`. Creation tries `[Type]::new(...)`, `New-Object`, and COM instantiation in order. A bare type literal (`"[System.IO.Path]"`) selects [static mode](#static-classes). |
| `static` | Force static mode for an unbracketed type name or a `$variable` that holds a type object. |

With a generated Protocol class as the first argument, the object is created
from the metadata embedded in the class, and the return value is typed as the
protocol — no annotation needed:

```python
from StringBuilder import StringBuilder

sb = shell.make_proxy(StringBuilder)          # created AND typed in one call
sb.Append("x")                                 # full IDE completion

bound = shell.make_proxy(StringBuilder, "$existing")   # bind + keep typing
```

Binding to a `$variable` that is null or undefined raises `ValueError`. A creation expression that no strategy can materialise raises `RuntimeError` with a per-strategy error report.

If the Protocol class carries `__ps_assembly__` (generated for types that live
outside the .NET runtime, such as a vendor SDK), `make_proxy` first loads that
assembly into the session and registers its directory with a process-wide
`AssemblyResolve` probe, so the stub is self-sufficient in a fresh session:

```python
from TiaPortal import TiaPortal          # generated with expression=...

with Shell(powershell_edition="desktop") as sh:
    tia = sh.make_proxy(TiaPortal)       # loads Siemens.Engineering.Base.dll, then creates
```

Note that `__ps_expression__` is what gets evaluated. A stub generated from a
bare `$variable` whose type has no parameterless constructor still embeds the
variable name, and `make_proxy(Proto)` then only works where that variable
exists; generate with `expression=` to embed a real creation expression.

`obj_ref` may also be a derived expression such as `"$tia.Projects"` or
`"$sb.Append('x')"`: it is evaluated once into a private variable, so later
member access does not re-run it. Every proxy exposes `ps_origin`, the
expression it was reached by (`"$tia"`, `"[T]::new(3)"`, `"$tia.Projects"`,
...), which `generate_psobject` uses for stubs of derived objects.

## Basic Usage

```python
from virtualshell import Shell

with Shell() as sh:
    # Create the object through the proxy...
    sb = sh.make_proxy("", "System.Text.StringBuilder")
    sb.Append("Hello ")
    sb.Append("World")
    print(sb.ToString())        # "Hello World"
    print(sb.Length)            # 11 (a real Python int)

    # ...or bind to an object you created yourself
    sh.run("$client = [System.Net.WebClient]::new()")
    client = sh.make_proxy("", "$client")
```

## Static Classes

A bare `[Type]` literal binds the *type itself* instead of creating an
instance. The proxy then exposes the type's **static** methods, properties
and constants (invoked with `::` in the session):

```python
with Shell() as sh:
    math = sh.make_proxy("", "[System.Math]")
    math.Sqrt(16.0)                     # 4.0
    math.PI                             # 3.141592653589793 (const field)

    path = sh.make_proxy("", "[System.IO.Path]")
    path.Combine("a", "b")              # "a\\b"

    dow = sh.make_proxy("", "[System.DayOfWeek]")
    dow.Monday                          # "Monday" (enum values are fields)
```

The motivating example — a WinForms message box:

```python
with Shell(initial_commands=["Add-Type -AssemblyName System.Windows.Forms"]) as sh:
    msg_box = sh.make_proxy("", "[System.Windows.Forms.MessageBox]")
    msg_box.Show("Hello from Python!", "Title")   # blocks until dismissed
```

Notes:

- Return values convert exactly like instance calls (the table below):
  scalars inline, `byte[]` via the bridge, complex values as sub-proxies.
- Writable static properties can be assigned; consts and `readonly` fields
  are read-only and raise `AttributeError` on assignment.
- Instance members are **not** available on a static proxy (and vice
  versa); the same type can have one proxy of each kind.
- `static=True` forces static mode when the name is unbracketed
  (`make_proxy("", "System.Math", static=True)`) or when binding a
  `$variable` that already holds a type object.

## Value Conversion

Every read is routed through a runtime type check in PowerShell:

| PowerShell value | Python result |
|------------------|---------------|
| `$null` | `None` |
| `[string]`, `[char]`, `[guid]`, `[version]`, `[uri]` | `str` |
| Integer types (`[int]`, `[long]`, `[byte]`, ...) | `int` |
| `[double]`, `[single]`, `[decimal]` | `float` |
| `[bool]` | `bool` |
| Enums (`[DayOfWeek]`, ...) | `str` (the enum name) |
| `[datetime]` | `datetime.datetime` |
| `[timespan]` | `datetime.timedelta` |
| `[byte[]]` | `bytes` — transferred via the Zero-Copy Bridge |
| Anything else | A new **sub-proxy** bound to the value |

Strings survive newlines, quotes and non-ASCII characters in both directions.

### Sub-proxies

Complex return values and properties come back as live proxies referencing the value in the session:

```python
sb = sh.make_proxy("", "System.Text.StringBuilder")
same = sb.Append("abc")     # StringBuilder.Append returns the builder
same.Append("def")          # still the same underlying object
print(sb.ToString())        # "abcdef"
```

### Binary data and buffers

`bytes` arguments and `byte[]` results move through shared memory, so multi-megabyte payloads are practical:

```python
ms = sh.make_proxy("", "System.IO.MemoryStream")
ms.Write(payload, 0, len(payload))      # bytes -> PowerShell via the bridge
data = ms.ToArray()                     # byte[] -> Python bytes via the bridge

# Mutable out-buffers: pass a bytearray, it is filled in place
ms.Position = 0
buffer = bytearray(5)
count = ms.Read(buffer, 0, 5)           # buffer now holds the bytes read
```

## Methods and Properties

- Methods take positional arguments only; keyword arguments raise `TypeError`.
- Overloads are matched by argument count; `.NET Task`/`ValueTask` methods are awaited automatically (`GetAwaiter().GetResult()`).
- Argument types marshalled inline: `None`, `bool`, `int`, `float`, `str`, `datetime`, `timedelta`, lists/tuples (`@(...)`), dicts (`@{...}`), other proxies (passed by reference), and `bytes` (via the bridge).
- Writable properties can be assigned (`ms.Position = 0`); assigning a read-only property raises `AttributeError`.
- Unknown attribute names raise `AttributeError`; failed PowerShell invocations raise `virtualshell.ExecutionError` with the original error text.
- Names that are not PowerShell members can still be assigned — they are stored Python-side on the proxy.

## Proxy API

| Member | Description |
|--------|-------------|
| `proxy.type_name` | The object's real PowerShell type name. |
| `proxy.ps_ref` | The `$variable` the proxy is bound to. |
| `proxy.proxy_schema()` | Dict with `TypeName`, `Methods` (name/overload count) and `Properties` (name/type/writable). |
| `proxy.proxy_multi_call(method, args)` | Batch many invocations into few PowerShell round-trips (see below). |
| `proxy.to_psobject(depth=2)` | CliXml snapshot of the object as a [`PSObject`](Zero-Copy%20Bridge.md#psobject), via the bridge. |
| `proxy.release()` | Remove the proxy's session variable (only when the proxy created it). |
| `dir(proxy)` | Lists members for IDE auto-completion. |

### Batch calls with proxy_multi_call

```python
with Shell(timeout_seconds=120) as sh:
    sw = sh.make_proxy("", "System.IO.StreamWriter('file.txt')")
    lines = [f"Line {i}" for i in range(1000)]

    sw.proxy_multi_call(sw.WriteLine, lines)   # one argument per call
    sw.Flush(); sw.Close()

    al = sh.make_proxy("", "System.Collections.ArrayList")
    al.proxy_multi_call(al.Add, ["a", "b", "c"])   # -> [0, 1, 2]
```

Pass a list for one-argument-per-call, or an `int` to repeat a no-argument call. Results are returned as JSON-decoded values.

### Snapshots

```python
sh.run("$user = [pscustomobject]@{ Name = 'Kim'; Id = 7 }")
user = sh.make_proxy("", "$user")
snapshot = user.to_psobject()      # PSObject: a detached copy
print(snapshot["Name"], snapshot["Id"])
```

Use the live proxy to *act on* the object, and `to_psobject()` when you want a plain data copy to keep or inspect in Python.

## Integrating with Generated Protocols

Pair `make_proxy` with [`generate_psobject`](generate_psobject.md) to inform type checkers:

```python
from WebClient import WebClient  # generated protocol
from virtualshell import Shell

with Shell() as sh:
    client: WebClient = sh.make_proxy("", "System.Net.WebClient")
    print(client.BaseAddress)
```

## Tips

- Proxies are only valid while the `Shell` is running; after `.stop()` the backing objects are gone.
- Schemas are cached per type per session — creating many proxies of the same type is cheap.
- One `Shell` shares a single Zero-Copy Bridge (created lazily on first binary transfer) — you do not need to manage it yourself.
- Call `release()` on short-lived proxies you create in a loop to avoid accumulating session variables.
