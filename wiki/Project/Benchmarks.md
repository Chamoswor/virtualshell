# Benchmarks

Benchmark run collected on **2026-09-20** against PowerShell 7. Raw artifacts live beside this page (`bench.json`, `bench.csv`, `zcb_bench.json`).

- **Python**: 3.14.5 (`...\.venv\Scripts\python.exe`)
- **Platform**: Windows 11 (build 26200), AMD64, Intel64 Family 6 Model 60 Stepping 3
- **Timeout**: 30 s default
- **Command under test**: `$x=1+1; $x | Out-Null`

## Configuration Highlights

| Parameter                       | Value                   |
| ---                             | ---                     |
| Batch sizes                     | 50, 100, 200            |
| Batch repeats                   | 3                       |
| Async fan-out (`async_n`)       | 50                      |
| Parallel shells                 | 4 (50 commands each)    |
| Output files                    | `bench.json`, `bench.csv` |

## Throughput And Latency

All timings are wall-clock; throughput reported as completed commands per second.

| Size | Sequential mean (ms) | Sequential thrpt | Batch per-cmd mean (ms) | Batch thrpt | Async latency mean (ms) | Async thrpt | Batch eff. | Async eff. |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 3.971 | 251.56 | 3.909 | 255.81 | 2.663 | 266.35 | 1.016 | 1.058 |
| 100 | 3.838 | 260.36 | 3.651 | 273.87 | 2.647 | 271.03 | 1.051 | 1.040 |
| 200 | 3.536 | 282.53 | 3.183 | 314.14 | 2.263 | 310.14 | 1.111 | 1.097 |

**Key takeaways**

- Async submission sustained up to **310 cmds/s**, with ~2.3 ms average latency at size 200.
- Batch execution is ~2-11% more efficient than sequential issuance depending on size.
- Single-command overhead holds steady around **3.3 ms**.
- Four parallel shells reached **619 cmds/s** aggregate (200 commands in 0.32 s).

## Command Type Mix

Representative workloads (10-20 iterations per type) confirm consistent latency across scenarios.

| Command | Mean (ms) | 95th percentile (ms) | Est. throughput |
| --- | --- | --- | --- |
| Simple echo | 3.07 - 11.44 | 4.09 - 80.50 | 87 - 325 cmds/s |
| Math operation | 3.31 - 3.96 | 3.67 - 5.00 | 253 - 302 cmds/s |
| Get date | 3.55 - 5.91 | 4.13 - 22.27 | 169 - 282 cmds/s |
| File operation | 6.01 - 13.15 | 6.91 - 64.87 | 76 - 166 cmds/s |
| Variable assignment | 3.16 - 3.99 | 3.92 - 5.72 | 251 - 316 cmds/s |

Ranges express the min/max across the tested batch sizes.

## Zero-Copy Bridge

Collected with `zcb_bench.py` (best of 3 runs per size, 64 MB frame, 4 MB chunks).

| Transfer | 1 MB | 8 MB | 32 MB |
| --- | --- | --- | --- |
| PowerShell → Python (bytes) | 144.5 MB/s | 384.7 MB/s | 412.6 MB/s |
| PowerShell → Python (memoryview) | 144.9 MB/s | 453.5 MB/s | 562.7 MB/s |
| Python → PowerShell (send) | 178.3 MB/s | 368.5 MB/s | 292.9 MB/s |

- `return_memoryview=True` avoids the final `bytes()` copy and pays off at
  larger payloads (+36% at 32 MB).
- A 4 KB send+receive round trip takes **~7.8 ms** (p50; mean 10.1 ms) —
  dominated by the two command dispatches, not the payload.
- Baseline: the same 1 MB payload as base64 over stdout runs at **14.0 MB/s**,
  so the bridge is roughly 10-40× faster for bulk data.

## Persistence Costs

Session save operations average **391 ms** (p50 344 ms, p95 700 ms). Budget roughly 0.4 s when snapshotting state between runs.

## Detailed Metrics

- Startup cost: **0.44 s** for the initial PowerShell host.
- Command submission overhead: **3.3 ms** independent of payload.
- Size scaling: throughput stabilises beyond 50 commands; per-command overhead flattens near **3.2 ms** even at 200 commands.

## Artifacts

- `bench.json` contains full structured results (env, per-size stats, persistence metrics).
- `bench.csv` offers concise tabular data suitable for spreadsheets.
- `zcb_bench.json` contains the zero-copy bridge results.
- `vs_bench.py` and `zcb_bench.py` are the benchmark harness scripts.
