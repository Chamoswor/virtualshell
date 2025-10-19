### Page: Performance Tips

- Prefer **batch** or **async** for many small commands to amortize round‑trips.
- Keep Python callbacks **lean**; heavy work should be offloaded.
- Avoid unnecessary object churn on hot paths.
- Use appropriate **timeouts** to avoid hanging commands blocking the queue.
- Save the session before running commands that might hang so a restart can recover state automatically.