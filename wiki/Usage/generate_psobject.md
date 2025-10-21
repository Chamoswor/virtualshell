# generate_psobject

`generate_psobject` is a helper bundled with `virtualshell.Shell` that inspects a PowerShell object and produces a Python `Protocol` capturing its public surface. This lets IDEs and type-checkers offer autocompletion and static analysis when you interact with PowerShell objects from Python.

## Prerequisites

- Install the `virtualshell` package and ensure the native extension is built.
- PowerShell 7 has to be available in `PATH`, or you must pass an explicit `powershell_path` to `Shell`.
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
4. **Collects metadata:** Once an object is available, the helper executes `Get-Member`, converts the results to JSON, and derives method/property metadata.
5. **Renders a protocol:** A minimal Python module is generated with imports, type annotations, and method/property signatures derived from the PowerShell metadata.

If all strategies fail to materialise an object, you will see a summary of attempted expressions to aid troubleshooting.

## Customising Behaviour

- **Timeouts:** Pass a higher `timeout_seconds` when constructing the shell if the target object takes time to create.
- **PowerShell path:** Use `Shell(powershell_path="C:/Program Files/PowerShell/7/pwsh.exe")` to reference a specific installation.
- **Result stripping:** `strip_results=True` trims trailing whitespace from PowerShell output; it is optional but helpful for clean JSON parsing.

## Using the Generated Protocol

After generation, import the new module and annotate your variables:

```python
from WebClient import WebClient
from virtualshell import Shell

shell = Shell().start()

proxy = shell.make_proxy("System.Net.WebClient")
client: WebClient = proxy  # type checker now knows the shape
print(client.BaseAddress)

shell.stop()
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
