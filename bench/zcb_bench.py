#!/usr/bin/env python3
# zcb_bench.py - Zero-Copy Bridge benchmark for virtualshell
#
# Measures:
# - PowerShell -> Python receive throughput, as bytes and as memoryview
# - Python -> PowerShell send (write) throughput
# - Small-payload round-trip latency
# - A base64-over-stdout baseline for context

import argparse
import json
import os
import statistics
import time

from virtualshell import Shell, ZeroCopyBridge

MB = 1024 * 1024


def mbps(nbytes: int, seconds: float) -> float:
    return (nbytes / MB) / seconds if seconds > 0 else 0.0


def best_of(fn, repeats: int) -> float:
    """Best (minimum) wall time over `repeats` runs: least-noise estimate."""
    return min(_timed(fn) for _ in range(repeats))


def _timed(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def bench_receive(shell, bridge, size: int, repeats: int, *, memoryview_mode: bool):
    shell.run(
        f"$vs_bench_out = [byte[]]::new({size}); "
        "[System.Random]::new(42).NextBytes($vs_bench_out)",
        raise_on_error=True, timeout=120,
    )
    def op():
        data = bridge.receive("vs_bench_out", timeout=120,
                              return_memoryview=memoryview_mode)
        assert len(data) == size
    t = best_of(op, repeats)
    shell.run("Remove-Variable vs_bench_out -ErrorAction SilentlyContinue")
    return {"seconds": t, "mb_per_s": mbps(size, t)}


def bench_send(shell, bridge, size: int, repeats: int):
    payload = os.urandom(size)
    def op():
        bridge.send(payload, "vs_bench_in", timeout=120)
    t = best_of(op, repeats)
    length = shell.run("$vs_bench_in.Length").out.strip()
    assert length == str(size), f"expected {size}, PowerShell saw {length}"
    shell.run("Remove-Variable vs_bench_in -ErrorAction SilentlyContinue")
    return {"seconds": t, "mb_per_s": mbps(size, t)}


def bench_small_roundtrip(shell, bridge, size: int = 4096, iterations: int = 20):
    payload = os.urandom(size)
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        bridge.send(payload, "vs_bench_small", timeout=30)
        data = bridge.receive("vs_bench_small", timeout=30)
        times.append(time.perf_counter() - t0)
        assert bytes(data) == payload
    shell.run("Remove-Variable vs_bench_small -ErrorAction SilentlyContinue")
    return {
        "size_bytes": size,
        "iterations": iterations,
        "mean_ms": 1000.0 * statistics.fmean(times),
        "p50_ms": 1000.0 * sorted(times)[len(times) // 2],
        "min_ms": 1000.0 * min(times),
    }


def bench_stdout_baseline(shell, size: int, repeats: int):
    """Reference: same transfer over stdout as base64 (the non-bridge path)."""
    import base64
    shell.run(
        f"$vs_bench_b64src = [byte[]]::new({size}); "
        "[System.Random]::new(42).NextBytes($vs_bench_b64src)",
        raise_on_error=True, timeout=120,
    )
    def op():
        out = shell.run("[Convert]::ToBase64String($vs_bench_b64src)",
                        timeout=120, raise_on_error=True).out
        data = base64.b64decode(out.strip())
        assert len(data) == size
    t = best_of(op, repeats)
    shell.run("Remove-Variable vs_bench_b64src -ErrorAction SilentlyContinue")
    return {"seconds": t, "mb_per_s": mbps(size, t)}


def main():
    ap = argparse.ArgumentParser(description="Zero-Copy Bridge benchmark")
    ap.add_argument("--sizes-mb", type=int, nargs="+", default=[1, 8, 32])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--frame-mb", type=int, default=64)
    ap.add_argument("--chunk-mb", type=int, default=4)
    ap.add_argument("--baseline-mb", type=int, default=1,
                    help="Payload size for the base64/stdout baseline (0 disables).")
    ap.add_argument("--json-out", type=str, default="")
    args = ap.parse_args()

    report = {"config": vars(args), "receive_bytes": {}, "receive_memoryview": {},
              "send": {}, "small_roundtrip": {}, "stdout_baseline": {}}

    with Shell(timeout_seconds=120) as shell:
        with ZeroCopyBridge(shell, frame_mb=args.frame_mb, chunk_mb=args.chunk_mb) as bridge:
            # Warm-up: JIT, Add-Type, first mmap touch.
            bridge.send(b"warmup", "vs_bench_warm")
            bridge.receive("vs_bench_warm")

            for size_mb in args.sizes_mb:
                size = size_mb * MB
                print(f"\n--- {size_mb} MB ---")

                r = bench_receive(shell, bridge, size, args.repeats, memoryview_mode=False)
                report["receive_bytes"][size_mb] = r
                print(f"PS -> Py (bytes):      {r['mb_per_s']:8.1f} MB/s  ({r['seconds']*1000:.1f} ms)")

                r = bench_receive(shell, bridge, size, args.repeats, memoryview_mode=True)
                report["receive_memoryview"][size_mb] = r
                print(f"PS -> Py (memoryview): {r['mb_per_s']:8.1f} MB/s  ({r['seconds']*1000:.1f} ms)")

                r = bench_send(shell, bridge, size, args.repeats)
                report["send"][size_mb] = r
                print(f"Py -> PS (send):       {r['mb_per_s']:8.1f} MB/s  ({r['seconds']*1000:.1f} ms)")

            print("\n--- small round trip (4 KB send+receive) ---")
            r = bench_small_roundtrip(shell, bridge)
            report["small_roundtrip"] = r
            print(f"mean={r['mean_ms']:.1f} ms | p50={r['p50_ms']:.1f} ms | min={r['min_ms']:.1f} ms")

            if args.baseline_mb:
                print(f"\n--- stdout/base64 baseline ({args.baseline_mb} MB) ---")
                r = bench_stdout_baseline(shell, args.baseline_mb * MB, args.repeats)
                report["stdout_baseline"][args.baseline_mb] = r
                print(f"PS -> Py via stdout:   {r['mb_per_s']:8.1f} MB/s  ({r['seconds']*1000:.1f} ms)")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\nWrote JSON: {args.json_out}")


if __name__ == "__main__":
    main()
