### Page: Security Notes

- **Do not** pass untrusted strings as raw commands.
- `pwsh()` quotes a **literal** safely. Prefer it for data‑as‑argument cases.
- Avoid leaking secrets via logs or exceptions; environment injection is explicit via `Shell(environment=...)`.