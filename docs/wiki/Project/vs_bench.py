#!/usr/bin/env python3
# vs_bench.py — Comprehensive, low-noise benchmark for virtualshell
#
# Notes:
# - Comments/docstrings in English (per your preference), CLI/help in English.
# - Designed to reflect library behavior fairly: separates payload from framework overhead.
# - Requires: virtualshell (Shell) to be importable in current environment.

import os
import sys
import gc
import time
import json
import math
import argparse
import platform
import statistics
from concurrent.futures import as_completed
from datetime import datetime
from concurrent.futures import Future
from typing import List

try:
    from virtualshell import Shell, BatchProgress, ExecutionResult
except Exception as e:
    print("Failed to import 'virtualshell'. Install and make it importable first.")
    raise

# ---------------------------
# Command payload templates
# ---------------------------

NOOP = "'' | Out-Null"  # explicit no-op (cheapest possible)
MICRO = "$x=1+1; $x | Out-Null"  # small deterministic operation
SLEEP_1MS = "Start-Sleep -Milliseconds 1"  # introduces external wait to test timing pipeline

COMMAND_PRESETS = {
    "noop": NOOP,
    "micro": MICRO,
    "sleep1ms": SLEEP_1MS,
}

# ---------------------------
# Utility helpers
# ---------------------------

def pct(samples, p):
    """Nearest-rank percentile for finite list [0..1], inclusive of endpoints."""
    if not samples:
        return float("nan")
    if p <= 0:
        return samples[0]
    if p >= 1:
        return samples[-1]
    i = int(round(p * (len(samples) - 1)))
    i = max(0, min(len(samples) - 1, i))
    return samples[i]

def summarize(name, samples_s):
    """Return common stats in milliseconds for a list of seconds."""
    xs = list(samples_s)
    xs.sort()
    mean = statistics.fmean(xs) if xs else float("nan")
    stdev = statistics.pstdev(xs) if len(xs) > 1 else 0.0
    return {
        "name": name,
        "n": len(xs),
        "mean_ms": 1000.0 * mean,
        "stdev_ms": 1000.0 * stdev,
        "p50_ms": 1000.0 * pct(xs, 0.50),
        "p95_ms": 1000.0 * pct(xs, 0.95),
        "p99_ms": 1000.0 * pct(xs, 0.99),
    }

def env_info():
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "python": sys.version.split()[0],
        "executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "impl": platform.python_implementation(),
    }

def warm_up(shell, n=6, cmd=NOOP):
    # A short warmup to populate JIT/caches on both ends.
    for _ in range(n):
        shell.run(cmd)

# ---------------------------
# Benchmarks
# ---------------------------

def benchmark_single_commands(shell, num_commands=100, cmd_template=None, progress_every=0):
    """Sequential runs of one command at a time; returns summary/times/results_count."""
    cmd_template = cmd_template or SLEEP_1MS  # default matches prior behavior
    times, failures = [], 0
    start_total = time.perf_counter()

    for i in range(num_commands):
        cmd = cmd_template.format(i=i) if "{i}" in cmd_template else cmd_template
        t0 = time.perf_counter()
        try:
            shell.run(cmd)
        except Exception:
            failures += 1
            raise
        finally:
            t1 = time.perf_counter()
            times.append(t1 - t0)

        if progress_every and i and (i % progress_every == 0):
            print(f"  sequential: {i}/{num_commands}")

    total_time = time.perf_counter() - start_total
    summ = summarize("single_sequential", times)
    summ.update({
        "total_s": total_time,
        "throughput_cmds_per_s": (num_commands / total_time) if total_time > 0 else 0.0,
        "failures": failures,
    })
    return summ, times

def benchmark_batch_commands(shell, batch_size=100, repeats=3, cmd_template=None, verify=False, progress_every=0):
    """Run `repeats` batches of size `batch_size`; measure wall clock per batch and per-cmd means."""
    cmd_template = cmd_template or MICRO
    batch_times, failures = [], 0
    last_count = None

    for r in range(repeats):
        commands = [(cmd_template.format(i=i) if "{i}" in cmd_template else cmd_template)
                    for i in range(batch_size)]
        t0 = time.perf_counter()
        try:
            # Fallback to sequential if run_batch is absent:
            if hasattr(shell, "run_batch"):
                results = shell.run_batch(commands)
            else:
                results = [shell.run(c) for c in commands]
        except Exception:
            failures += 1
            raise
        finally:
            t1 = time.perf_counter()
            batch_times.append(t1 - t0)

        if verify:
            assert isinstance(results, (list, tuple)), "run_batch should return a sequence"
            assert len(results) == batch_size, f"Expected {batch_size}, got {len(results)}"
            last_count = len(results)

        if progress_every and (r + 1) % progress_every == 0:
            print(f"  batch: {r+1}/{repeats} ({batch_times[-1]:.4f}s)")

    summ = summarize("batch_wall_time", batch_times)
    mean_wall_s = (summ["mean_ms"] / 1000.0) if summ["n"] else float("inf")
    per_cmd_mean_s = (mean_wall_s / batch_size) if batch_size > 0 else float("inf")
    summ.update({
        "batches": repeats,
        "batch_size": batch_size,
        "total_cmds": repeats * batch_size,
        "total_wall_s": sum(batch_times),
        "per_batch_mean_s": mean_wall_s,
        "per_cmd_mean_s": per_cmd_mean_s,
        "throughput_cmds_per_s": (batch_size / mean_wall_s) if mean_wall_s > 0 else 0.0,
        "failures": failures,
        "verified_last_batch_count": last_count,
    })
    return summ, batch_times

def benchmark_async_commands(shell: Shell, num_commands=100, cmd_template=None, progress_every=0):
    """Submit N async commands to a single Shell queue; collect per-future latencies and total wall."""
    cmd_template = cmd_template or MICRO
    commands = [(cmd_template.format(i=i) if "{i}" in cmd_template else cmd_template)
                for i in range(num_commands)]
    starts = {}
    failures = 0




    futures: List[ExecutionResult] = []

    submit_t0 = time.perf_counter()
    fut: Future[List[ExecutionResult]] = shell.run_async(commands)
    futures = fut.result()

    latencies = []
    completed = 0
    for result in futures:
        if result.success:
            completed += 1
        else:
            failures += 1
        latencies.append(result.execution_time)

    wall = time.perf_counter() - submit_t0
    summ = summarize("async_single_shell_latency", latencies)
    summ.update({
        "wall_time_s": wall,
        "throughput_cmds_per_s": (completed / wall) if wall > 0 else 0.0,
        "submitted_n": num_commands,
        "completed_n": completed,
        "failures": failures,
    })
    return summ, latencies

def benchmark_command_types(shell, num_commands=20, progress_every=0):
    """Cycle through several lightweight commands to characterize opcode mix."""
    kinds = {
        "simple_echo": 'Write-Output "test"',
        "math_operation": '1 + 1',
        "get_date": 'Get-Date',
        "file_operation": 'Get-ChildItem | Select-Object -First 1',
        "variable_assignment": '$x = 5; $x * 2',
    }
    raw_times = {k: [] for k in kinds}
    t0 = time.perf_counter()

    for i in range(num_commands):
        for kind, cmd in kinds.items():
            s = time.perf_counter()
            shell.run(cmd)
            e = time.perf_counter()
            raw_times[kind].append(e - s)
        if progress_every and i and (i % progress_every == 0):
            print(f"  types: {i}/{num_commands}")

    total_wall = time.perf_counter() - t0
    summaries = {}
    for kind, samples in raw_times.items():
        s = summarize(kind, samples)
        mean_s = s["mean_ms"] / 1000.0
        s.update({
            "per_cmd_mean_s": mean_s,
            "est_throughput_cmds_per_s": (1.0 / mean_s) if mean_s > 0 else 0.0,
            "n": len(samples),
        })
        summaries[kind] = s
    summaries["_total"] = {
        "mixed_wall_time_s": total_wall,
        "iterations": num_commands,
        "types": list(kinds.keys())
    }
    return summaries, raw_times

def benchmark_session_persistence(shell: Shell, num_operations=10):
    """Measure save/restore cost if available."""
    times = []
    if not hasattr(shell, "save_session"):
        return summarize("session_save", times), times

    state_commands = [
        '$testVar = "persisted_data"',
        'Add-Content -Path "test.txt" -Value "test_data"',
        'Remove-Item "test.txt" -ErrorAction SilentlyContinue',
    ]
    for i in range(num_operations):
        for cmd in state_commands:
            shell.run(cmd)
        s = time.perf_counter()
        shell.save_session()
        e = time.perf_counter()
        times.append(e - s)
    return summarize("session_save", times), times

def analyze_performance_characteristics():
    """Startup time (ctx manager), command overhead (NOOP), and output size impact."""
    # Startup
    s0 = time.perf_counter()
    with Shell(timeout_seconds=30, auto_restart_on_timeout=True) as sh:
        startup_time = time.perf_counter() - s0
        sh.run('Write-Output "warmup"')

        # Overhead of an "empty-ish" command
        overhead_times = []
        for _ in range(24):
            a = time.perf_counter()
            sh.run(NOOP)
            b = time.perf_counter()
            overhead_times.append(b - a)
        overhead_mean = statistics.fmean(overhead_times)

        # Output size impact
        sizes = [10, 100, 1000, 10_000]
        size_times = []
        for n in sizes:
            cmd = f'Write-Output ("x" * {n})'
            a = time.perf_counter()
            sh.run(cmd)
            b = time.perf_counter()
            size_times.append((n, b - a))

    return {
        "startup_time_s": startup_time,
        "command_overhead_s": overhead_mean,
        "size_impact": size_times,
    }

def benchmark_parallel_shells(num_shells=4, cmds_per_shell=50, cmd=MICRO):
    shells = [Shell(timeout_seconds=30) for _ in range(num_shells)]
    for sh in shells:
        sh.start()
    try:
        futs = []
        commands = [cmd for _ in range(cmds_per_shell)]
        t0 = time.perf_counter()
        for sh in shells:
            futs.append(sh.run_async(commands))
        for f in as_completed(futs):
            f.result()
        wall = time.perf_counter() - t0
        total = num_shells * cmds_per_shell
        return {
            "shells": num_shells,
            "total_cmds": total,
            "throughput_cmds_per_s": total / wall if wall > 0 else 0.0,
            "wall_time_s": wall,
        }
    finally:
        for sh in shells:
            sh.stop()

# ---------------------------
# Orchestrator
# ---------------------------

def run_all(cfg):
    report = {
        "env": env_info(),
        "config": vars(cfg),
        "per_size": {},
        "persistence": {},
        "detailed": {},
        "parallel": {},
        "assessment": {},
    }

    # Keep GC behavior consistent during hot loops
    prev_gc = gc.isenabled()
    gc.disable()

    try:
        with Shell(timeout_seconds=cfg.timeout) as shell:
            warm_up(shell, n=6, cmd=NOOP)

            for size in cfg.sizes:
                print(f"\n{'='*40}\nTESTING WITH {size} COMMANDS\n{'='*40}")
                # Sequential
                single_summ, single_times = benchmark_single_commands(
                    shell, num_commands=size, cmd_template=cfg.cmd, progress_every=0
                )
                single_mean_s = single_summ["mean_ms"] / 1000.0

                # Batch
                batch_summ, batch_times = benchmark_batch_commands(
                    shell, batch_size=size, repeats=cfg.batch_repeats,
                    cmd_template=cfg.cmd, verify=True, progress_every=0
                )

                # Async (limit noise on very large sizes)
                async_n = min(size, cfg.async_n)
                async_summ, async_lat = benchmark_async_commands(
                    shell, num_commands=async_n, cmd_template=cfg.cmd, progress_every=0
                )

                # Command types (optional)
                type_summaries, _ = ({}, {})
                if cfg.types:
                    type_summaries, _ = benchmark_command_types(
                        shell, num_commands=min(20, max(10, size // 5)), progress_every=0
                    )

                # Efficiency metrics
                batch_per_cmd_mean = batch_summ["per_cmd_mean_s"]
                batch_eff = (single_mean_s / batch_per_cmd_mean) if batch_per_cmd_mean > 0 else 0.0
                async_eff = ((single_mean_s * async_n) / async_summ["wall_time_s"]) if async_summ["wall_time_s"] > 0 else 0.0

                # Print compact result
                print("\n--- RESULTS ---")
                print(f"Single (sequential): total={single_summ['total_s']:.3f}s | "
                    f"mean={single_summ['mean_ms']:.1f} ms | p50={single_summ['p50_ms']:.1f} | "
                    f"p95={single_summ['p95_ms']:.1f} | p99={single_summ['p99_ms']:.1f} | "
                    f"thr={single_summ['throughput_cmds_per_s']:.1f} cmd/s")

                print(f"Batch (repeats={cfg.batch_repeats}): mean wall/batch={batch_summ['per_batch_mean_s']:.3f}s | "
                    f"total(all)={batch_summ['total_wall_s']:.3f}s | p50={batch_summ['p50_ms']:.1f} | "
                    f"p95={batch_summ['p95_ms']:.1f} | p99={batch_summ['p99_ms']:.1f} | "
                    f"thr={batch_summ['throughput_cmds_per_s']:.1f} cmd/s")

                print(f"Async (single-shell queue, n={async_n}): wall={async_summ['wall_time_s']:.3f}s | "
                    f"lat mean={async_summ['mean_ms']:.1f} ms | p50={async_summ['p50_ms']:.1f} | "
                    f"p95={async_summ['p95_ms']:.1f} | p99={async_summ['p99_ms']:.1f} | "
                    f"thr={async_summ['throughput_cmds_per_s']:.1f} cmd/s")

                if type_summaries:
                    print("\nCommand type summaries (ms):")
                    for k, s in type_summaries.items():
                        if k == "_total": continue
                        print(f"  {k:18s} mean={s['mean_ms']:.1f} | p50={s['p50_ms']:.1f} | "
                            f"p95={s['p95_ms']:.1f} | p99={s['p99_ms']:.1f}")

                print("\nEfficiencies:")
                print(f"  batch_efficiency: {batch_eff:.2f}x (vs single mean per cmd)")
                print(f"  async_efficiency: {async_eff:.2f}x (vs single mean × n / async wall)")

                report["per_size"][size] = {
                    "single": single_summ,
                    "batch": batch_summ,
                    "async": async_summ,
                    "types": type_summaries,
                    "efficiency": {
                        "batch_efficiency": batch_eff,
                        "async_efficiency": async_eff,
                    },
                }

            # Persistence
            print(f"\n{'='*40}\nSESSION PERSISTENCE\n{'='*40}")
            pers_summ, pers_raw = benchmark_session_persistence(shell, num_operations=10)
            print(f"Session save: mean={pers_summ.get('mean_ms', float('nan')):.1f} ms | "
                f"p50={pers_summ.get('p50_ms', float('nan')):.1f} | "
                f"p95={pers_summ.get('p95_ms', float('nan')):.1f} | "
                f"p99={pers_summ.get('p99_ms', float('nan')):.1f} ms")
            report["persistence"] = {"summary": pers_summ, "raw": pers_raw}

    finally:
        if prev_gc:
            gc.enable()

    # Detailed (fresh shell to include startup)
    detailed = analyze_performance_characteristics()
    report["detailed"] = detailed

    # Parallel scaling (optional)
    if cfg.num_shells > 0 and cfg.cmds_per_shell > 0:
        par = benchmark_parallel_shells(cfg.num_shells, cfg.cmds_per_shell, cmd=cfg.cmd)
        report["parallel"] = par
        print(f"\nParallel shells: {par['shells']} shells, {par['total_cmds']} cmds "
              f"| wall={par['wall_time_s']:.3f}s | thr={par['throughput_cmds_per_s']:.1f} cmd/s")
    
    if not all:
        return report
    # Assessment
    batch_effs = [v["efficiency"]["batch_efficiency"] for v in report["per_size"].values()]
    async_effs = [v["efficiency"]["async_efficiency"] for v in report["per_size"].values() if v["efficiency"]["async_efficiency"] > 0]
    avg_batch_eff = statistics.fmean(batch_effs) if batch_effs else 0.0
    avg_async_eff = statistics.fmean(async_effs) if async_effs else 0.0

    overhead = detailed["command_overhead_s"]
    startup = detailed["startup_time_s"]

    report["assessment"] = {
        "avg_batch_efficiency": avg_batch_eff,
        "avg_async_efficiency": avg_async_eff,
        "command_overhead_s": overhead,
        "startup_time_s": startup,
    }

    print(f"\n{'='*50}\nPERFORMANCE ASSESSMENT\n{'='*50}")
    print(f"Batch efficiency avg: {avg_batch_eff:.2f}x — "
          f"{'Efficient' if avg_batch_eff > 1.2 else ('Moderate' if avg_batch_eff > 0.8 else 'Inefficient')}")
    print(f"Async efficiency avg: {avg_async_eff:.2f}x — "
          f"{'Highly efficient' if avg_async_eff > 1.5 else ('Efficient' if avg_async_eff > 1.0 else 'Moderate')}")
    print(f"Command overhead: {overhead*1000:.1f} ms — "
          f"{'Low' if overhead < 0.010 else ('Moderate' if overhead < 0.050 else 'High')}")
    print(f"Startup time: {startup:.2f} s — "
          f"{'Fast' if startup < 1.0 else ('Moderate' if startup < 3.0 else 'Slow')}")

    return report

# ---------------------------
# CLI
# ---------------------------

def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Comprehensive benchmark for the virtualshell library."
    )
    ap.add_argument("--sizes", type=int, nargs="+", default=[50, 100, 200],
                    help="Command counts to test per scenario.")
    ap.add_argument("--batch-repeats", type=int, default=3,
                    help="How many batches to run for batch benchmark.")
    ap.add_argument("--async-n", type=int, default=50,
                    help="Number of async commands per async benchmark (capped by size).")
    ap.add_argument("--cmd", type=str, default="micro",
                    help="Command payload: 'noop', 'micro', 'sleep1ms', or a literal PowerShell snippet.")
    ap.add_argument("--types", action="store_true",
                    help="Also run command-type characterization.")
    ap.add_argument("--timeout", type=float, default=30.0,
                    help="Shell timeout_seconds.")
    ap.add_argument("--num-shells", type=int, default=0,
                    help="Parallel shells to test (>1 enables scaling test).")
    ap.add_argument("--cmds-per-shell", type=int, default=50,
                    help="Commands per shell for parallel benchmark.")
    ap.add_argument("--json-out", type=str, default="",
                    help="If set, write the full report as JSON to this path.")
    ap.add_argument("--csv-out", type=str, default="",
                    help="If set, write a compact CSV of the main per-size results.")
    return ap.parse_args(argv)

def resolve_cmd_arg(cmd_arg: str) -> str:
    # Accept preset key or literal PS code
    key = cmd_arg.strip().lower()
    return COMMAND_PRESETS.get(key, cmd_arg)

def write_csv(path, report):
    # Compact CSV: one row per size with key metrics
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "size",
            "single_mean_ms", "single_thr",
            "batch_per_cmd_mean_ms", "batch_thr",
            "async_lat_mean_ms", "async_thr",
            "batch_eff", "async_eff"
        ])
        for size, data in sorted(report["per_size"].items()):
            single = data["single"]
            batch = data["batch"]
            asyncs = data["async"]
            eff = data["efficiency"]
            w.writerow([
                size,
                f"{single['mean_ms']:.3f}", f"{single['throughput_cmds_per_s']:.3f}",
                f"{batch['per_cmd_mean_s']*1000.0:.3f}", f"{batch['throughput_cmds_per_s']:.3f}",
                f"{asyncs['mean_ms']:.3f}", f"{asyncs['throughput_cmds_per_s']:.3f}",
                f"{eff['batch_efficiency']:.3f}", f"{eff['async_efficiency']:.3f}",
            ])

def main(argv=None):
    cfg = parse_args(argv)
    cfg.cmd = resolve_cmd_arg(cfg.cmd)

    print("VirtualShell Comprehensive Benchmark")
    print("Note: warmup + low-noise payloads to reflect module overhead fairly.")
    print(f"Env: {env_info()['platform']} | Python {env_info()['python']}")
    report = run_all(cfg)

    if cfg.json_out:
        with open(cfg.json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"Wrote JSON: {cfg.json_out}")

    if cfg.csv_out:
        write_csv(cfg.csv_out, report)
        print(f"Wrote CSV:  {cfg.csv_out}")

if __name__ == "__main__":
    try:
        main()
        print("\nBenchmark completed successfully!")
    except Exception as e:
        print(f"\nBenchmark failed with error: {e}")
        import traceback
        traceback.print_exc()
