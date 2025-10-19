### Page: Design & Architecture

- Python wrapper delegates all heavy I/O and orchestration to a C++ core.
- Clear failure modes and minimal Python‑side locking.
- Boundary hygiene: minimal data marshalling; explicit conversions for paths/args.
- Asynchronous command execution with futures and optional callbacks.
- Per-command timeouts to prevent blocking the command queue. (Note: see Performance Tips for usage recommendations.)
- Robust restart handling to recover from hangs or crashes.
- Modular design with separate components for I/O pumping, command state, and shell management.
- Extensive logging and error reporting for easier debugging and maintenance.
- Comprehensive test coverage to ensure reliability and correctness.
- Designed for performance with batch command execution and minimal overhead.
