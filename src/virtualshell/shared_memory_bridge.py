"""Utilities for moving large payloads between Python and PowerShell using shared memory."""
from __future__ import annotations

import importlib
import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union, Literal, TYPE_CHECKING

try:
    _CPP_MODULE = importlib.import_module(f"{__package__}._core")
except Exception as exc:  # pragma: no cover
    raise ImportError(
        "Failed to import the compiled extension 'virtualshell._core'. "
        "Make sure it was built and matches this Python/platform."
    ) from exc

SharedMemoryChannel = _CPP_MODULE.SharedMemoryChannel

if TYPE_CHECKING:  # pragma: no cover
    from .shell import Shell

__all__ = ["SharedMemoryBridge", "SharedMemoryPublishResult"]

BytesLike = Union[bytes, bytearray, memoryview]
OutboundFormat = Literal["bytes", "text", "json"]
InboundFormat = Literal["bytes", "text", "json"]

_DEFAULT_POLL_INTERVAL = 0.01
_DEFAULT_POLL_ATTEMPTS = 200


def _quote_pwsh_literal(value: str) -> str:
    """Return a PowerShell single-quoted literal."""
    return "'" + value.replace("'", "''") + "'"


def _ensure_bytes(data: BytesLike) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    return bytes(memoryview(data))


def _ensure_shell_started(shell: "Shell") -> None:
    if not shell.is_running:
        shell.start()


def _module_dir() -> Path:
    return Path(__file__).resolve().parent


@dataclass(frozen=True)
class SharedMemoryPublishResult:
    channel_name: str
    frame_bytes: int
    data_length: int
    sequence: int


class SharedMemoryBridge:
    """High-level helper that moves large payloads between Python and PowerShell."""

    def __init__(
        self,
        shell: "Shell",
        *,
        default_frame_bytes: int = 1024 * 1024,
        default_slots: int = 1,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        poll_attempts: int = _DEFAULT_POLL_ATTEMPTS,
    ) -> None:
        self._shell = shell
        self._default_frame_bytes = int(default_frame_bytes)
        self._default_slots = int(default_slots)
        self._poll_interval = float(poll_interval)
        self._poll_attempts = int(poll_attempts)
        self._ps_ready = False

        if self._default_frame_bytes <= 0:
            raise ValueError("default_frame_bytes must be positive")
        if self._default_slots <= 0:
            raise ValueError("default_slots must be positive")
        if self._poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if self._poll_attempts <= 0:
            raise ValueError("poll_attempts must be positive")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def publish(
        self,
        data: Union[BytesLike, str, Any],
        *,
        variable: str,
        format: OutboundFormat = "bytes",
        encoding: str = "utf-8",
        frame_bytes: Optional[int] = None,
        channel_name: Optional[str] = None,
    ) -> SharedMemoryPublishResult:
        """Publish *data* to a PowerShell variable via shared memory.

        Parameters
        ----------
        data:
            Payload to send. When ``format='bytes'`` the value must be bytes-like.
            For ``format='text'`` any object is converted via ``str(data)``.
            For ``format='json'`` the value is serialised with ``json.dumps``.
        variable:
            Name of the PowerShell variable that will receive the data.
        format:
            ``'bytes'`` (default), ``'text'`` or ``'json'``.
        encoding:
            Text/JSON encoding when ``format`` is not ``'bytes'``.
        frame_bytes:
            Optional explicit frame size. Defaults to payload length.
        channel_name:
            Optional channel name. A random unique name is used when omitted.
        """

        self._ensure_ready()
        payload = self._encode_outbound(data, format, encoding)
        payload_len = len(payload)
        if payload_len == 0 and frame_bytes is None:
            raise ValueError("Provide frame_bytes when publishing empty payloads")

        frame_size = int(frame_bytes or payload_len)
        if payload_len > frame_size:
            raise ValueError("Payload exceeds requested frame size")
        if frame_size <= 0:
            raise ValueError("frame_bytes must be positive")

        chan_name = channel_name or self._make_channel_name()
        channel = SharedMemoryChannel(
            chan_name,
            self._default_slots,
            frame_size,
        )
        try:
            buffer = bytearray(frame_size)
            buffer[:payload_len] = payload
            channel.write_to_powershell(bytes(buffer))
            seq_value = int(channel.get_python_seq())
            if seq_value <= 0:
                raise RuntimeError("Shared memory sequence did not advance after publish")
            sequence = seq_value - 1

            ps_cmd = self._build_import_command(
                chan_name,
                frame_size,
                variable,
                payload_len,
                sequence,
                format,
                encoding,
            )
            self._shell.run(ps_cmd, raise_on_error=True)
            return SharedMemoryPublishResult(chan_name, frame_size, payload_len, sequence)
        finally:
            del channel

    def fetch(
        self,
        command: str,
        *,
        format: InboundFormat = "json",
        encoding: str = "utf-8",
        frame_bytes: Optional[int] = None,
        channel_name: Optional[str] = None,
    ) -> Any:
        """Execute *command* in PowerShell and return its data via shared memory."""

        self._ensure_ready()
        frame_size = int(frame_bytes or self._default_frame_bytes)
        if frame_size <= 0:
            raise ValueError("frame_bytes must be positive")
        name = channel_name or self._make_channel_name()

        channel = SharedMemoryChannel(name, self._default_slots, frame_size)
        try:
            ps_cmd = self._build_export_command(
                name,
                frame_size,
                command,
                format,
                encoding,
            )
            res = self._shell.run(ps_cmd, raise_on_error=True)
            lines = [line.strip() for line in res.out.splitlines() if line.strip()]
            declared_len: Optional[int] = None
            for line in reversed(lines):
                if line.isdigit():
                    declared_len = int(line)
                    break
            data_length = declared_len if declared_len is not None else frame_size
            if data_length > frame_size:
                raise ValueError(
                    f"PowerShell reported data length {data_length} exceeding frame size {frame_size}."
                )

            seq = self._wait_for_powershell(channel)
            raw = channel.read_from_powershell(seq - 1)
            payload = raw[:data_length]
            return self._decode_inbound(payload, format, encoding)
        finally:
            del channel

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure_ready(self) -> None:
        if self._ps_ready:
            return
        _ensure_shell_started(self._shell)
        module_dir = _module_dir()
        if not getattr(self._shell, "_vshell_shared_memory_loaded", False):
            self._shell.script(module_dir / "shared_memory.ps1", dot_source=True)
            setattr(self._shell, "_vshell_shared_memory_loaded", True)
        if not getattr(self._shell, "_vshell_shared_memory_bridge_loaded", False):
            self._shell.script(module_dir / "shared_memory_bridge.ps1", dot_source=True)
            setattr(self._shell, "_vshell_shared_memory_bridge_loaded", True)
        self._ps_ready = True

    def _encode_outbound(self, data: Any, format: OutboundFormat, encoding: str) -> bytes:
        fmt = format.lower()
        if fmt == "bytes":
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError("Expected bytes-like object when format='bytes'")
            return _ensure_bytes(data)
        if fmt == "text":
            return str(data).encode(encoding)
        if fmt == "json":
            return json.dumps(data).encode(encoding)
        raise ValueError(f"Unsupported format '{format}'")

    def _decode_inbound(self, payload: bytes, format: InboundFormat, encoding: str) -> Any:
        fmt = format.lower()
        if fmt == "bytes":
            return payload
        if fmt == "text":
            return payload.decode(encoding)
        if fmt == "json":
            if not payload:
                return None
            return json.loads(payload.decode(encoding))
        raise ValueError(f"Unsupported format '{format}'")

    def _build_import_command(
        self,
        channel_name: str,
        frame_bytes: int,
        variable: str,
        data_length: int,
        sequence: int,
        fmt: OutboundFormat,
        encoding: str,
    ) -> str:
        fmt_token = {"bytes": "Bytes", "text": "String", "json": "Json"}[fmt.lower()]
        return (
            "Import-SharedMemoryData "
            f"-ChannelName {_quote_pwsh_literal(channel_name)} "
            f"-FrameBytes {frame_bytes} "
            f"-NumSlots {self._default_slots} "
            f"-Sequence {sequence} "
            f"-Format {_quote_pwsh_literal(fmt_token)} "
            f"-Encoding {_quote_pwsh_literal(encoding)} "
            f"-DataLength {data_length} "
            f"-VariableName {_quote_pwsh_literal(variable)}"
        )

    def _build_export_command(
        self,
        channel_name: str,
        frame_bytes: int,
        command: str,
        fmt: InboundFormat,
        encoding: str,
    ) -> str:
        fmt_token = {"bytes": "Bytes", "text": "String", "json": "Json"}[fmt.lower()]
        return (
            "Export-SharedMemoryData "
            f"-ChannelName {_quote_pwsh_literal(channel_name)} "
            f"-FrameBytes {frame_bytes} "
            f"-NumSlots {self._default_slots} "
            f"-Format {_quote_pwsh_literal(fmt_token)} "
            f"-Encoding {_quote_pwsh_literal(encoding)} "
            f"-Command {_quote_pwsh_literal(command)}"
        )

    def _make_channel_name(self) -> str:
        return f"vsbridge_{uuid.uuid4().hex}"

    def _wait_for_powershell(self, channel: SharedMemoryChannel) -> int:
        for _ in range(self._poll_attempts):
            seq = channel.get_powershell_seq()
            if seq > 0:
                return int(seq)
            time.sleep(self._poll_interval)
        raise TimeoutError("Timed out waiting for PowerShell to populate shared memory")
