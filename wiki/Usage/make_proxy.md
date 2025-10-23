# make_proxy

`Shell.make_proxy` materialises a dynamic Python object that forwards attribute access to a PowerShell instance. It is the ergonomic way to work with .NET / PowerShell objects while staying in Python.

## Signature

```python
proxy = shell.make_proxy(type_name: str,
                         object_expression: str,
                         *,
                         depth: int = 4)
```

| Parameter | Description |
|-----------|-------------|
| `type_name` | Friendly name assigned to the proxy type (used for error messages and `__type_name__`). |
| `object_expression` | PowerShell expression that resolves to the underlying object (for example `"$client"`. |
| `depth` | Controls how deep `Get-Member` metadata is harvested (default `4`). Increase when nested members expose additional structure. |

The call returns a live proxy object. Attribute reads and method invocations transparently run in PowerShell, and results are coerced back to Python scalars when possible.

## Basic Usage

```python
from virtualshell import Shell

with Shell(strip_results=True) as sh:
    sh.run("$client = [System.Net.WebClient]::new()")
    client = sh.make_proxy("WebClientProxy", "$client")

    content = client.DownloadString("https://www.example.com")
    print(content[:120])
```

## Integrating with Generated Protocols

Pair `make_proxy` with [`generate_psobject`](generate_psobject.md) to preserve type information:

```python
from WebClient import WebClient  # generated protocol
from virtualshell import Shell

with Shell() as sh:
    proxy = sh.make_proxy("WebClientProxy", "System.Net.WebClient")
    client: WebClient = proxy  # type checker now knows the shape

    print(client.BaseAddress)
```



## Optimizations implemented
- Member metadata is cached per proxy type to avoid repeated `Get-Member` calls.
- Repeated method calls with the same argument types reuse prepared PowerShell scripts for speed. Usage:

### Example - Basic File Writing/Reading
```python
from virtualshell import Shell
with Shell(strip_results=True, timeout_seconds=120) as sh:
    sw = sh.make_proxy("StreamWriterProxy", "System.IO.StreamWriter('file.txt')")

    for i in range(5):
        sw.WriteLine(f"Line {i}")

    sw.Flush()
    sw.Close()
    sw.Dispose()

    sr = sh.make_proxy("StreamReaderProxy", "System.IO.StreamReader('file.txt')")
    while not sr.EndOfStream:
        print(sr.ReadLine())

    sr.Close()
    sr.Dispose()
```

### Example - Using proxy_multi_call for batch calls
```python
from virtualshell import Shell
with Shell(strip_results=True, timeout_seconds=120, stdin_buffer_size=640 * 1024) as sh:
    sw = sh.make_proxy("StreamWriterProxy", "System.IO.StreamWriter('file.txt')")
    num_lines = 1000
    lines = [f"Line {i}" for i in range(num_lines)]

    # Batch write 1000 lines in one PowerShell call
    sw.proxy_multi_call(sw.WriteLine, lines)

    sw.Flush()
    sw.Close()
    sw.Dispose()

    sr = sh.make_proxy("StreamReaderProxy", "System.IO.StreamReader('file.txt')")
    r: list[str] = sr.proxy_multi_call(sr.ReadLine, num_lines)

    # Print the first 100 lines
    for line in r[:100]:
        print(line)

    sr.Close()
    sr.Dispose()
```

## Attributes Available on a Proxy

- Regular properties call into PowerShell (including updating values if the property is writable).
- Methods support positional arguments; asynchronous .NET Task-returning methods are awaited automatically.
- `proxy_schema` returns a dictionary of member names to their types (as strings).
- `proxy_multi_call` enables batching multiple method calls into a single PowerShell invocation for performance.
- `__getattr__` and `__setattr__` support dynamic member access.
- `__dict__` exposes a per-proxy dictionary for dynamic Python-side state.
- `__dir__` lists available members for IDE auto-completion.
- `__repr__` shows the proxy type and underlying PowerShell expression.
## Error Handling

- Missing members raise `AttributeError`.
- PowerShell invocation failures raise `ValueError` with the original PowerShell error text.
- Argument conversion uses the same literal formatting as synchronous `Shell.run`; unsupported types raise `TypeError`.

## Tips

- Keep your proxies alive only while the `Shell` is running. After `.stop()` the backing object is no longer valid.
- Use `depth` selectively; very deep `Get-Member` calls can be slow for large graphs.
- Combine with Python's `typing.cast` to inform type-checkers about the protocol you expect the proxy to satisfy.
- Use `proxy_multi_call` for high-frequency method invocations to reduce inter-process overhead.
