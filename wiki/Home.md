### Page: Home

**VirtualShell** is a high-level Python wrapper around a C++ core that hosts PowerShell. It provides configuration, lifecycle management, sync/async execution, and clean Python exceptions.

**Highlights**
- Thin Python façade over a fast C++ core
- Single commands, batches, scripts (positional or named args)
- Structured results: `run_objects()` returns Python objects, not text to parse
- Futures‑based async with optional callbacks
- Timeouts and automatic restart of the backend process (opt‑in)
- Typed error translation (timeout/error/blocked prompt) and simple success checks
- Agent guardrails: `ExecutionPolicy` with allow/deny lists, read-only lanes, confirmation hooks and automatic `-WhatIf` dry runs
- Output budgets for LLM contexts: `max_output` + `fetch_output` paging
- `interrupt()` cancels a runaway command in ~0.5 s and restores session state from the newest checkpoint
- Checkpoints: named save/restore points for session state (`checkpoint`/`restore`)
- MCP-style tool schemas from `Get-Command` metadata (`command_schema`/`module_schemas`)
- PowerShell object proxies for type-safe access to complex objects
- Session save/restore for crash recovery
- Configurable environment, working directory, UTF-8 mode, and more

New in 1.3: see [Agents & Guardrails](Agents-&-Guardrails).
