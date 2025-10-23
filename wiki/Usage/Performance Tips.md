### Page: Performance Tips

- Prefer **batch** or **async** for many small commands to amortize round‑trips.
- Keep Python callbacks **lean**; heavy work should be offloaded.
- Avoid unnecessary object churn on hot paths.
- Use appropriate **timeouts** to avoid hanging commands blocking the queue.
- Save the session before running commands that might hang so a restart can recover state automatically.
- Use `strip_results=True` on `Shell` if you don't need full stdout/stderr details to reduce data transfer.
- Use `make_proxy` for **type-safe** access to complex PowerShell objects.
- Use `generate_psobject` to create static Protocol definitions for complex PowerShell objects for better type checking and IDE support.
- Use `proxy.proxy_multi_call` to batch multiple method calls into a single PowerShell invocation for performance.
- Increase `stdin_buffer_size` on `Shell` if sending large scripts or data to avoid fragmentation.


