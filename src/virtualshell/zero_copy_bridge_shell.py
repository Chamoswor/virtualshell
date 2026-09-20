"""Zero-Copy Bridge - cross-platform shared memory transfer with PowerShell.

Design:
- Python owns the channel lifecycle: it creates a file-backed memory map that
  both sides open (Python via ``mmap``, PowerShell via .NET
  ``MemoryMappedFile``). This works on Windows, Linux and macOS; no native
  DLL is required.
- The existing ``Shell`` command channel coordinates transfers; bulk data
  moves through the shared mapping in chunks with ready/ack flags.
- Layout and protocol must match zero_copy_bridge.ps1 (see the header there).
"""
from __future__ import annotations

import mmap
import os
import secrets
import struct
import tempfile
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Any, List, Optional, TYPE_CHECKING, Union, overload

from .ps_object import PSObject

if TYPE_CHECKING:
    from .shell import Shell

__all__ = ["ZeroCopyBridge", "PSObject"]

_IS_WINDOWS = os.name == "nt"

# =============================================================================
# SHARED MEMORY CHANNEL (protocol v3)
# =============================================================================

_MAGIC = b"VSZB"
_VERSION = 3
_HEADER_SIZE = 256

# Direction block base offsets in the header.
_BLK_PY2PS = 16   # Python writes, PowerShell reads
_BLK_PS2PY = 80   # PowerShell writes, Python reads

# Field offsets within a direction block.
_F_TOTAL = 0    # int64 total transfer size
_F_CHUNK = 8    # int64 chunk size
_F_NUM = 16     # int32 number of chunks
_F_IDX = 20     # int32 current chunk index
_F_LEN = 24     # int64 current chunk length
_F_READY = 32   # int32 1 = chunk available; reader clears to 0 as ack
_F_DONE = 36    # int32 1 = writer finished all chunks
_F_ERR = 40     # int32 non-zero = transfer aborted (1=python, 2=powershell)

_ERR_PY_ABORT = 1

_POLL_SLEEP = 0.0005


class _MmapChannel:
    """File-backed shared memory channel implementing protocol v3.

    The creator writes the header and owns the file; a peer (normally the
    PowerShell side, or a second instance in tests) opens the same file.
    One chunk is in flight per direction; flags are polled, which is portable
    and fast enough at multi-megabyte chunk sizes.
    """

    def __init__(self, path: Union[str, Path], frame_bytes: int = 0, *, create: bool = True):
        self.path = Path(path)
        if create:
            if frame_bytes <= 0:
                raise ValueError("frame_bytes must be positive when creating a channel")
            self.frame_bytes = int(frame_bytes)
            total = _HEADER_SIZE + 2 * self.frame_bytes
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "wb") as f:
                f.truncate(total)
            self._file = open(self.path, "r+b")
            self._mm = mmap.mmap(self._file.fileno(), total)
            struct.pack_into("<4sIq", self._mm, 0, _MAGIC, _VERSION, self.frame_bytes)
        else:
            self._file = open(self.path, "r+b")
            self._mm = mmap.mmap(self._file.fileno(), 0)
            magic, version, frame = struct.unpack_from("<4sIq", self._mm, 0)
            if magic != _MAGIC:
                raise ValueError(f"Not a virtualshell channel file: {self.path}")
            if version != _VERSION:
                raise ValueError(f"Channel protocol version mismatch: {version}")
            self.frame_bytes = frame

    # -- low-level field access ------------------------------------------------

    def _ri32(self, off: int) -> int:
        return struct.unpack_from("<i", self._mm, off)[0]

    def _wi32(self, off: int, value: int) -> None:
        struct.pack_into("<i", self._mm, off, value)

    def _ri64(self, off: int) -> int:
        return struct.unpack_from("<q", self._mm, off)[0]

    def _wi64(self, off: int, value: int) -> None:
        struct.pack_into("<q", self._mm, off, value)

    def _block_and_data(self, direction: str) -> tuple[int, int]:
        if direction == "py2ps":
            return _BLK_PY2PS, _HEADER_SIZE
        if direction == "ps2py":
            return _BLK_PS2PY, _HEADER_SIZE + self.frame_bytes
        raise ValueError(f"Unknown direction: {direction}")

    def _check_peer_error(self, blk: int) -> None:
        err = self._ri32(blk + _F_ERR)
        if err not in (0, _ERR_PY_ABORT):
            raise RuntimeError(f"Peer aborted the transfer (error={err})")

    # -- public protocol operations -------------------------------------------

    def reset(self, direction: str) -> None:
        """Clear a direction block before dispatching the peer's operation.

        Removes stale ready/done/error flags from a previous transfer so the
        reader cannot observe them before the writer's own reset.
        """
        blk, _ = self._block_and_data(direction)
        for field in (_F_NUM, _F_IDX, _F_READY, _F_DONE, _F_ERR):
            self._wi32(blk + field, 0)
        self._wi64(blk + _F_TOTAL, 0)
        self._wi64(blk + _F_LEN, 0)

    def send(self, data: bytes, *, chunk_size: int, timeout: float,
             direction: str = "py2ps") -> None:
        """Write `data` as a chunked transfer (writer role)."""
        blk, data_off = self._block_and_data(direction)
        chunk_size = max(1, min(int(chunk_size), self.frame_bytes))
        total = len(data)
        num_chunks = (total + chunk_size - 1) // chunk_size if total else 0

        self._wi64(blk + _F_TOTAL, total)
        self._wi64(blk + _F_CHUNK, chunk_size)
        self._wi32(blk + _F_NUM, num_chunks)
        self._wi32(blk + _F_IDX, 0)
        self._wi64(blk + _F_LEN, 0)
        self._wi32(blk + _F_READY, 0)
        self._wi32(blk + _F_DONE, 0)
        self._wi32(blk + _F_ERR, 0)

        view = memoryview(data)
        deadline = time.monotonic() + timeout
        for i in range(num_chunks):
            offset = i * chunk_size
            length = min(chunk_size, total - offset)
            self._mm[data_off:data_off + length] = view[offset:offset + length]
            self._wi32(blk + _F_IDX, i)
            self._wi64(blk + _F_LEN, length)
            self._wi32(blk + _F_READY, 1)

            while self._ri32(blk + _F_READY) == 1:
                self._check_peer_error(blk)
                if time.monotonic() > deadline:
                    self._wi32(blk + _F_ERR, _ERR_PY_ABORT)
                    raise TimeoutError(f"Timed out waiting for ack on chunk {i}")
                time.sleep(_POLL_SLEEP)
            self._check_peer_error(blk)

        self._wi32(blk + _F_DONE, 1)

    def receive(self, *, timeout: float, direction: str = "ps2py") -> bytearray:
        """Read a full chunked transfer (reader role)."""
        blk, data_off = self._block_and_data(direction)
        deadline = time.monotonic() + timeout
        result: Optional[bytearray] = None
        received = 0
        written = 0

        while True:
            self._check_peer_error(blk)

            if self._ri32(blk + _F_READY) == 1:
                if result is None:
                    result = bytearray(self._ri64(blk + _F_TOTAL))
                length = self._ri64(blk + _F_LEN)
                result[written:written + length] = self._mm[data_off:data_off + length]
                written += length
                received += 1
                self._wi32(blk + _F_READY, 0)  # ack
                if received == self._ri32(blk + _F_NUM):
                    return result
                continue

            if self._ri32(blk + _F_DONE) == 1:
                # Covers the empty-payload case (num_chunks == 0).
                if result is None:
                    result = bytearray(self._ri64(blk + _F_TOTAL))
                if received >= self._ri32(blk + _F_NUM):
                    return result

            if time.monotonic() > deadline:
                self._wi32(blk + _F_ERR, _ERR_PY_ABORT)
                raise TimeoutError("Timed out waiting for chunk from peer")
            time.sleep(_POLL_SLEEP)

    def close(self) -> None:
        for attr in ("_mm", "_file"):
            obj = getattr(self, attr, None)
            if obj is not None:
                try:
                    obj.close()
                except Exception:
                    pass
                setattr(self, attr, None)

    def unlink(self) -> None:
        """Close and delete the backing file (creator side)."""
        self.close()
        # Windows refuses deletion while any mapping is open; the PowerShell
        # side must Dispose first (Close-VsZcb). Retry briefly to absorb that.
        for _ in range(20):
            try:
                self.path.unlink(missing_ok=True)
                return
            except OSError:
                time.sleep(0.05)

    def __enter__(self) -> "_MmapChannel":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# =============================================================================
# INTEGRATED BRIDGE WITH SHELL
# =============================================================================

class ZeroCopyBridge:
    """Cross-platform bridge that moves bulk data past the stdio pipes.

    Uses the Shell command channel for coordination and a file-backed shared
    memory map for the payload.

    Example:
        from virtualshell import Shell, ZeroCopyBridge

        with Shell() as shell, ZeroCopyBridge(shell) as bridge:
            # PowerShell -> Python
            shell.run("$data = [byte[]](1..100)")
            payload = bridge.receive("data")

            # Python -> PowerShell
            bridge.send(b"hello", "greeting")   # $greeting is byte[] in PS
    """

    def __init__(
        self,
        shell: "Shell",
        frame_mb: int = 64,
        chunk_mb: int = 4,
        scope: str = "Local",   # kept for backwards compatibility; unused
    ):
        """Initialize the bridge on a running Shell (started if necessary).

        Args:
            shell: Shell instance controlling the PowerShell session.
            frame_mb: Data region size per direction, in MB.
            chunk_mb: Default chunk size, in MB.
            scope: Ignored (the v2 Windows-only named-object scope).
        """
        del scope
        self.shell = shell
        self.frame_bytes = frame_mb * 1024 * 1024
        self.default_chunk_bytes = min(chunk_mb * 1024 * 1024, self.frame_bytes)
        self._active_futures: List[Future[Any]] = []
        self._channel: Optional[_MmapChannel] = None

        channel_dir = Path(tempfile.gettempdir()) / "virtualshell"
        self.channel_path = channel_dir / f"zcb_{os.getpid()}_{secrets.token_hex(8)}.bin"
        self._channel = _MmapChannel(self.channel_path, self.frame_bytes, create=True)

        try:
            if not self.shell.is_running:
                self.shell.start()

            script = Path(__file__).resolve().parent / "zero_copy_bridge.ps1"
            if _IS_WINDOWS:
                self.shell.run(
                    "Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force")
            self.shell.run(f". '{script}'", raise_on_error=True)
            self.shell.run(
                f"Initialize-VsZcb -Path '{self.channel_path}'", raise_on_error=True)
        except Exception:
            self._channel.unlink()
            self._channel = None
            raise

    # -- lifecycle -------------------------------------------------------------

    def close(self) -> None:
        """Release the channel on both sides and delete the backing file."""
        for future in self._active_futures:
            future.cancel()
        self._active_futures.clear()

        if self._channel is not None:
            try:
                if self.shell.is_running:
                    self.shell.run("Close-VsZcb", timeout=5.0)
            except Exception:
                pass
            self._channel.unlink()
            self._channel = None

    def __enter__(self) -> "ZeroCopyBridge":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _require_channel(self) -> _MmapChannel:
        if self._channel is None:
            raise RuntimeError("ZeroCopyBridge is closed")
        return self._channel

    def _track_future(self, future: Future[Any]) -> Future[Any]:
        self._active_futures.append(future)

        def _remove(_fut: Future[Any]) -> None:
            try:
                self._active_futures.remove(_fut)
            except ValueError:
                pass

        future.add_done_callback(_remove)
        return future

    # -- serialization helpers -------------------------------------------------

    def serialize(
        self,
        variable: str,
        *,
        depth: int = 1,
        out_var: Optional[str] = None,
        timeout: float = 30.0,
    ) -> bool:
        """Serialize a PowerShell variable to CliXml bytes (in-place by default).

        After this, the (out) variable holds byte[] which `receive()` can
        transfer and `PSObject.from_bytes` can parse.
        """
        var_name = variable.lstrip("$")
        out_name = out_var.lstrip("$") if out_var is not None else var_name

        results = self.shell.run(
            [
                f"$__vs_xml = [System.Management.Automation.PSSerializer]::Serialize(${var_name}, {int(depth)})",
                f"${out_name} = [System.Text.Encoding]::UTF8.GetBytes($__vs_xml)",
            ],
            timeout=timeout,
        )
        return all(r.success for r in results)

    def deserialize(
        self,
        variable: str,
        *,
        out_var: Optional[str] = None,
        timeout: float = 30.0,
    ) -> bool:
        """Deserialize CliXml bytes in a PowerShell variable back to an object."""
        var_name = variable.lstrip("$")
        out_name = out_var.lstrip("$") if out_var is not None else var_name

        results = self.shell.run(
            [
                f"$__vs_xml = [System.Text.Encoding]::UTF8.GetString(${var_name})",
                f"${out_name} = [System.Management.Automation.PSSerializer]::Deserialize($__vs_xml)",
            ],
            timeout=timeout,
        )
        return all(r.success for r in results)

    @overload
    def receive(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
        depth: int = 1,
    ) -> bytes:
        ...

    @overload
    def receive(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
        return_memoryview: bool,
        depth: int = 1,
    ) -> memoryview:
        ...

    # =========================================================================
    # POWERSHELL -> PYTHON
    # =========================================================================
    def receive(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
        return_memoryview: bool = False,
        depth: int = 1,
    ) -> Union[bytes, memoryview]:
        """Transfer a PowerShell variable's bytes to Python.

        byte[] variables are sent as-is; any other value is serialized to
        CliXml by the PowerShell side first (parse with PSObject.from_bytes).
        `depth` controls PSSerializer nesting for the non-byte[] case.
        """
        channel = self._require_channel()
        var_name = variable.lstrip("$")

        # Clear stale flags before the PowerShell writer starts.
        channel.reset("ps2py")

        chunk_mb = max(1, self.default_chunk_bytes // (1024 * 1024))
        future = self._track_future(self.shell.run_async(
            f"Send-VariableToPython -Variable ${var_name} "
            f"-ChunkSizeMB {chunk_mb} -TimeoutSeconds {int(timeout)} "
            f"-Depth {int(depth)}",
            timeout=timeout,
        ))

        try:
            data = channel.receive(timeout=timeout, direction="ps2py")
        except Exception:
            future.cancel()
            raise

        result = future.result(timeout=timeout)
        if not result.success:
            raise RuntimeError(f"PowerShell send failed: {result.err}")

        return memoryview(data) if return_memoryview else bytes(data)

    # =========================================================================
    # PYTHON -> POWERSHELL
    # =========================================================================

    def send(
        self,
        data: bytes,
        variable: str,
        *,
        chunk_size: Optional[int] = None,
        timeout: float = 30.0,
    ) -> None:
        """Transfer bytes from Python into a PowerShell byte[] variable."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data must be bytes-like")

        channel = self._require_channel()
        var_name = variable.lstrip("$")

        # Clear stale flags before the PowerShell reader starts polling.
        channel.reset("py2ps")

        future = self._track_future(self.shell.run_async(
            f"Receive-VariableFromPython -VariableName '{var_name}' "
            f"-TimeoutSeconds {int(timeout)}",
            timeout=timeout,
        ))

        try:
            channel.send(
                bytes(data) if not isinstance(data, bytes) else data,
                chunk_size=chunk_size or self.default_chunk_bytes,
                timeout=timeout,
                direction="py2ps",
            )
        except Exception:
            future.cancel()
            raise

        result = future.result(timeout=timeout)
        if not result.success:
            raise RuntimeError(f"PowerShell receive failed: {result.err}")
