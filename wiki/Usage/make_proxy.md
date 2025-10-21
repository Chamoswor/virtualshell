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
| `object_expression` | PowerShell expression that resolves to the underlying object (for example `"$client"` or `"[System.Net.WebClient]::new()"`). |
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
    sh.run("$client = New-Object System.Net.WebClient")
    proxy = sh.make_proxy("WebClientProxy", "$client")
    client: WebClient = proxy  # type checker now knows the shape

    print(client.BaseAddress)
```

## Attributes Available on a Proxy

- Regular properties call into PowerShell (including updating values if the property is writable).
- Methods support positional arguments; asynchronous .NET Task-returning methods are awaited automatically.
- `__members__` returns a schema describing discovered methods and properties.
- `__dict__` exposes a per-proxy dictionary for dynamic Python-side state.
- `dir(proxy)` enumerates PowerShell members plus any dynamic attributes.

## Error Handling

- Missing members raise `AttributeError`.
- PowerShell invocation failures raise `ValueError` with the original PowerShell error text.
- Argument conversion uses the same literal formatting as synchronous `Shell.run`; unsupported types raise `TypeError`.

## Tips

- Keep your proxies alive only while the `Shell` is running. After `.stop()` the backing object is no longer valid.
- Use `depth` selectively; very deep `Get-Member` calls can be slow for large graphs.
- Combine with Python's `typing.cast` to inform type-checkers about the protocol you expect the proxy to satisfy.
