"""Performance test harness for the zero-copy Shell bridge.

The PowerShell v2 bridge moves data in chunks through shared memory.  This
script drives end-to-end transfers in both directions so we can collect basic
throughput numbers for large payloads.
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Sequence

from concurrent.futures import Future

from virtualshell import Shell
from virtualshell.zero_copy_bridge_shell import ZeroCopyBridge


if os.name != "nt":
    raise SystemExit("Zero-copy bridge performance tests currently require Windows.")


# Start with smaller sizes since transfer is slow (~2 MB/s)
DEFAULT_TEST_SIZES_MB: list[int] = [10, 50, 100]
ENV_SIZE_OVERRIDE = "VIRTUALSHELL_PERF_SIZES"


def format_speed(bytes_transferred, seconds):
    """Format transfer speed in MB/s."""
    mb = bytes_transferred / (1024 * 1024)
    return f"{mb / seconds:.2f} MB/s"


def format_size(bytes_size):
    """Format size in MB."""
    return f"{bytes_size / (1024 * 1024):.0f} MB"


def _parse_size_list(raw: str) -> list[int]:
    sizes: list[int] = []
    for chunk in raw.replace(";", ",").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        value = int(chunk)
        if value <= 0:
            raise ValueError(f"Size must be positive, got {value}")
        sizes.append(value)
    if not sizes:
        raise ValueError("No valid sizes specified")
    return sizes


def _resolve_sizes(sizes_override: Sequence[int] | None) -> list[int]:
    if sizes_override:
        return list(dict.fromkeys(int(s) for s in sizes_override))
    env_value = os.environ.get(ENV_SIZE_OVERRIDE)
    if env_value:
        return _parse_size_list(env_value)
    return DEFAULT_TEST_SIZES_MB.copy()


def test_powershell_to_python(shell: Shell, bridge: ZeroCopyBridge, size_mb: int, timeout_seconds: float) -> float | None:
    """Test PowerShell → Python transfer speed."""
    size_bytes = size_mb * 1024 * 1024

    print(f"  Preparing {size_mb} MB in PowerShell...", end=" ", flush=True)
    prep_start = time.perf_counter()
    shell.run(f"$testData = [byte[]]::new({size_bytes})")
    prep_time = time.perf_counter() - prep_start
    print(f"done ({prep_time:.2f}s)")

    print(f"  Transferring PS → Python...", end=" ", flush=True)
    start = time.perf_counter()
    transfer = bridge.send_from_powershell("$testData", use_job=False, timeout=timeout_seconds)
    if not isinstance(transfer, Future):
        raise RuntimeError("send_from_powershell did not return Future in async mode")

    try:
        print(f"waiting for {size_mb} MB...", end=" ", flush=True)
        received = bridge.receive(timeout=timeout_seconds, return_memoryview=True)
    finally:
        try:
            transfer.result(timeout=timeout_seconds)
        except Exception as exc:  # pragma: no cover - diagnostic output
            print(f"\n  ✗ PowerShell send failed: {exc}")
            return None

    shell.run("$testData = $null; [System.GC]::Collect() | Out-Null")
    elapsed = time.perf_counter() - start
    print("done")

    received_len = len(received)
    if received_len == size_bytes:
        print(f"  ✓ Received {format_size(received_len)}")
    else:
        print(f"  ✗ Size mismatch! Expected {size_bytes}, got {received_len}")
        return None

    speed = format_speed(size_bytes, elapsed)
    print(f"  ⚡ Speed: {speed} ({elapsed:.3f}s)")

    return elapsed


def test_python_to_powershell(shell: Shell, bridge: ZeroCopyBridge, size_mb: int, timeout_seconds: float) -> float | None:
    """Test Python → PowerShell transfer speed."""
    size_bytes = size_mb * 1024 * 1024

    print(f"  Preparing {size_mb} MB in Python...", end=" ", flush=True)
    prep_start = time.perf_counter()
    try:
        test_data = bytes(size_bytes)
    except MemoryError:
        print("skipped (MemoryError)")
        return None
    prep_time = time.perf_counter() - prep_start
    print(f"done ({prep_time:.2f}s)")

    print(f"  Transferring Python → PS...", end=" ", flush=True)
    start = time.perf_counter()
    transfer = bridge.receive_to_powershell("$receivedData", use_job=False, timeout=timeout_seconds)
    if not isinstance(transfer, Future):
        raise RuntimeError("receive_to_powershell did not return Future in async mode")

    try:
        print(f"sending {size_mb} MB...", end=" ", flush=True)
        bridge.send(test_data, timeout=timeout_seconds)
    finally:
        try:
            transfer.result(timeout=timeout_seconds)
        except Exception as exc:  # pragma: no cover - diagnostic output
            print(f"\n  ✗ PowerShell receive failed: {exc}")
            return None

    elapsed = time.perf_counter() - start
    print("done")

    result = shell.run("$receivedData.Length")
    shell.run("$receivedData = $null; [System.GC]::Collect() | Out-Null")
    try:
        received_size = int(result.out.strip())
    except ValueError:
        received_size = -1

    if received_size == size_bytes:
        print(f"  ✓ PowerShell received {format_size(received_size)}")
    else:
        print(f"  ✗ Size mismatch! Expected {size_bytes}, got {received_size}")
        return None

    speed = format_speed(size_bytes, elapsed)
    print(f"  ⚡ Speed: {speed} ({elapsed:.3f}s)")

    return elapsed


def run_performance_tests(
    sizes_mb: Sequence[int] | None = None,
    *,
    frame_mb: int = 256,
    chunk_mb: int = 32,
    timeout_seconds: float = 300.0,
    scope: str = "Local",
) -> None:
    """Run comprehensive performance tests."""

    test_sizes = _resolve_sizes(sizes_mb)

    print("=" * 70)
    print("ZERO-COPY BRIDGE PERFORMANCE TEST")
    print("=" * 70)
    print(f"Testing sizes: {', '.join(str(s) + ' MB' for s in test_sizes)}")
    print()

    results_ps2py: dict[int, float] = {}
    results_py2ps: dict[int, float] = {}

    shell_timeout = max(int(timeout_seconds * 2), 300)

    with Shell(timeout_seconds=shell_timeout, set_UTF8=True) as shell:
        shell.run("$ProgressPreference = 'SilentlyContinue'")

        print("\n" + "=" * 70)
        print("POWERSHELL → PYTHON")
        print("=" * 70)

        for size_mb in test_sizes:
            print(f"\nTest: {size_mb} MB")
            with ZeroCopyBridge(shell, channel_name=f"perf_ps2py_{size_mb}", frame_mb=frame_mb, chunk_mb=chunk_mb, scope=scope) as bridge:
                try:
                    elapsed = test_powershell_to_python(shell, bridge, size_mb, timeout_seconds)
                except Exception as exc:
                    print(f"  ✗ Error during PS→PY transfer: {exc}")
                    elapsed = None
                if elapsed:
                    results_ps2py[size_mb] = elapsed

            time.sleep(0.5)

        print("\n" + "=" * 70)
        print("PYTHON → POWERSHELL")
        print("=" * 70)

        for size_mb in test_sizes:
            print(f"\nTest: {size_mb} MB")
            with ZeroCopyBridge(shell, channel_name=f"perf_py2ps_{size_mb}", frame_mb=frame_mb, chunk_mb=chunk_mb, scope=scope) as bridge:
                try:
                    elapsed = test_python_to_powershell(shell, bridge, size_mb, timeout_seconds)
                except Exception as exc:
                    print(f"  ✗ Error during PY→PS transfer: {exc}")
                    elapsed = None
                if elapsed:
                    results_py2ps[size_mb] = elapsed

            time.sleep(0.5)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print("\nPowerShell → Python:")
    print(f"{'Size':<10} {'Time':<10} {'Speed':<15}")
    print("-" * 35)
    for size_mb, elapsed in results_ps2py.items():
        size_bytes = size_mb * 1024 * 1024
        speed = format_speed(size_bytes, elapsed)
        print(f"{size_mb} MB{'':<4} {elapsed:.3f}s{'':<3} {speed}")

    print("\nPython → PowerShell:")
    print(f"{'Size':<10} {'Time':<10} {'Speed':<15}")
    print("-" * 35)
    for size_mb, elapsed in results_py2ps.items():
        size_bytes = size_mb * 1024 * 1024
        speed = format_speed(size_bytes, elapsed)
        print(f"{size_mb} MB{'':<4} {elapsed:.3f}s{'':<3} {speed}")

    if results_ps2py:
        total_bytes_ps2py = sum(size_mb * 1024 * 1024 for size_mb in results_ps2py)
        avg_speed_ps2py = format_speed(total_bytes_ps2py, sum(results_ps2py.values()))
        avg_ps2py = sum(results_ps2py.values()) / len(results_ps2py)
        print(f"\nAverage PS→PY: {avg_ps2py:.3f}s per transfer, {avg_speed_ps2py} overall")

    if results_py2ps:
        total_bytes_py2ps = sum(size_mb * 1024 * 1024 for size_mb in results_py2ps)
        avg_speed_py2ps = format_speed(total_bytes_py2ps, sum(results_py2ps.values()))
        avg_py2ps = sum(results_py2ps.values()) / len(results_py2ps)
        print(f"Average PY→PS: {avg_py2ps:.3f}s per transfer, {avg_speed_py2ps} overall")

    print("\n" + "=" * 70)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run zero-copy bridge performance tests")
    parser.add_argument("--sizes", type=str, help="Comma-separated list of payload sizes in MB")
    parser.add_argument("--frame-mb", type=int, default=256, help="Shared memory frame size in MB (per direction)")
    parser.add_argument("--chunk-mb", type=int, default=32, help="Chunk size in MB")
    parser.add_argument("--timeout", type=float, default=300.0, help="Timeout for each transfer in seconds")
    parser.add_argument("--scope", type=str, default="Local", choices=["Local", "Global"], help="Shared memory scope prefix")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    sizes = _parse_size_list(args.sizes) if args.sizes else None

    run_performance_tests(
        sizes,
        frame_mb=args.frame_mb,
        chunk_mb=args.chunk_mb,
        timeout_seconds=args.timeout,
        scope=args.scope,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as exc:  # pragma: no cover - diagnostic output
        print(f"\n✗ ERROR: {exc}")
        import traceback

        traceback.print_exc()
