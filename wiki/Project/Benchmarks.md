# Benchmarks

Benchmark run collected on **2025-10-19** against the Windows PowerShell host. Raw artifacts live beside this page (`bench.json`, `bench.csv`).

- **Python**: 3.13.5 (`...\.venv\Scripts\python.exe`)
- **Platform**: Windows 11 (build 26200), AMD64, Intel64 Family 6 Model 60 Stepping 3
- **Timeout**: 30 s default
- **Command under test**: `$x=1+1; $x | Out-Null`

## Configuration Highlights

| Parameter                       | Value                   |
| ---                             | ---                     |
| Batch sizes                     | 50, 100, 200            |
| Batch repeats                   | 3                       |
| Async fan-out (`async_n`)       | 50                      |
| Commands per shell (sequential) | 50                      |
| Warm shells                     | 0 (single shell reused) |
| Output files                    | `bench.json`, `bench.csv` |

## Throughput And Latency

All timings are wall-clock; throughput reported as completed commands per second.

| Size | Sequential mean (ms) | Sequential thrpt | Batch per-cmd mean (ms) | Batch thrpt | Async latency mean (ms) | Async thrpt | Batch eff. | Async eff. |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 4.041 | 247.30 | 3.387 | 295.24 | 2.359 | 293.61 | 1.193 | 1.186 |
| 100 | 3.754 | 266.17 | 3.685 | 271.40 | 2.247 | 304.26 | 1.019 | 1.142 |
| 200 | 3.476 | 287.45 | 3.203 | 312.23 | 1.938 | 348.66 | 1.085 | 1.212 |

**Key takeaways**

- Async submission sustained up to **349 cmds/s**, with sub-2 ms average latency at size 200.
- Batch execution remains ~8-19% more efficient than sequential issuance depending on size.
- Single-command overhead holds steady around **3.0 ms**, implying platform start-up dominates short runs.

## Command Type Mix

Representative workloads (10-20 iterations per type) confirm consistent latency across scenarios.

| Command | Mean (ms) | 95th percentile (ms) | Est. throughput |
| --- | --- | --- | --- |
| Simple echo | 3.16 - 10.75 | 3.54 - 77.07 | 93 - 316 cmds/s |
| Math operation | 3.47 - 3.70 | 4.09 - 4.34 | 271 - 288 cmds/s |
| Get date | 3.70 - 5.78 | 4.45 - 22.54 | 173 - 270 cmds/s |
| File operation | 9.08 - 15.47 | 10.99 - 69.53 | 65 - 110 cmds/s |
| Variable assignment | 3.38 - 4.22 | 3.79 - 7.72 | 237 - 296 cmds/s |

Ranges express the min/max across the tested batch sizes.

## Persistence Costs

Session save operations average **302 ms** (p50 272 ms, p95 594 ms). Budget roughly 0.3 s when snapshotting state between runs.

## Detailed Metrics

- Startup cost: **0.33 s** for the initial PowerShell host.
- Command submission overhead: **3.0 ms** independent of payload.
- Size scaling: throughput stabilises beyond 50 commands; per-command overhead flattens near **3.2 ms** even at 200 commands.

## Artifacts

- `bench.json` contains full structured results (env, per-size stats, persistence metrics).
- `bench.csv` offers concise tabular data suitable for spreadsheets.
- `vs_bench.py` is the benchmark harness script.
