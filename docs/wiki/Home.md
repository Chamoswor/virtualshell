### Page: Home

**VirtualShell** is a high-level Python wrapper around a C++ core that hosts PowerShell. It provides configuration, lifecycle management, sync/async execution, and clean Python exceptions.

**Highlights**
- Thin Python façade over a fast C++ core
- Single commands, batches, scripts (positional or named args)
- Futures‑based async with optional callbacks
- Timeouts and automatic restart of the backend process (opt‑in)
- Typed error translation (timeout/error) and simple success checks