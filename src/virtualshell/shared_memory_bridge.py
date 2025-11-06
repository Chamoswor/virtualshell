"""Zero-copy shared memory bridge for Python ↔ PowerShell data transfer."""
from __future__ import annotations

import importlib
import json
import math
import mmap
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, Union, TYPE_CHECKING

try:
    _core = importlib.import_module(f"{__package__}._core")
except Exception as exc:
    raise ImportError(
        f"Failed to import {__package__}._core extension module. "
        "Ensure the native extension was built successfully."
    ) from exc

if TYPE_CHECKING:
    from .shell import Shell

__all__ = ["SharedMemoryBridge", "PublishResult", "ZeroCopyView"]

# Type aliases
BytesLike = Union[bytes, bytearray, memoryview]
Format = Literal["bytes", "text", "json"]

# Constants
POLL_INTERVAL = 0.01
POLL_ATTEMPTS = 200
HEADER_BYTES = int(_core.SharedMemoryChannel.header_bytes)

# Regex for PowerShell variable references like $global:Var or $MyVar
VAR_REF_RE = re.compile(
    r"^\s*\$(?:(?P<scope>(?:global|script|local|private):))?(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*$",
    re.IGNORECASE,
)


def _quote_ps(s: str) -> str:
    """Quote string for PowerShell single-quoted literal."""
    return "'" + s.replace("'", "''") + "'"


def _as_bytes(data: BytesLike) -> bytes:
    """Convert bytes-like to bytes."""
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    return bytes(memoryview(data))


def _module_dir() -> Path:
    """Get directory containing this module."""
    return Path(__file__).resolve().parent


@dataclass
class ZeroCopyView:
    """Zero-copy view over shared memory.
    
    Provides direct memoryview access to shared memory without copying data
    to Python heap. Call close() when done to release resources.
    
    Attributes
    ----------
    channel_name : str
        Shared memory channel name
    memoryview : memoryview
        Direct view into shared memory (no copy)
    length : int
        Actual data length in bytes
    frame_bytes : int
        Total frame size allocated
    """
    channel_name: str
    memoryview: memoryview
    length: int
    frame_bytes: int
    _mmap: mmap.mmap
    _channel: Any  # Keep channel alive

    def to_bytes(self) -> bytes:
        """Copy data to Python bytes (defeats zero-copy)."""
        return self.memoryview[:self.length].tobytes()

    def close(self) -> None:
        """Release all resources."""
        try:
            self.memoryview.release()
        except Exception:
            pass
        try:
            self._mmap.close()
        except Exception:
            pass
        try:
            del self._channel
        except Exception:
            pass


@dataclass(frozen=True)
class PublishResult:
    """Result of publishing data to PowerShell.
    
    Attributes
    ----------
    channel_name : str
        Shared memory channel name
    frame_bytes : int
        Frame size allocated
    data_length : int
        Actual data length
    sequence : int
        Write sequence number
    zero_copy : bool
        Whether zero-copy mode was used
    """
    channel_name: str
    frame_bytes: int
    data_length: int
    sequence: int
    zero_copy: bool


class SharedMemoryBridge:
    """Zero-copy shared memory bridge for Python ↔ PowerShell.
    
    Supports two modes:
    
    1. **Zero-copy** (recommended for large payloads >1 MiB):
       - publish() with zero_copy=True creates SharedMemoryBuffer in PowerShell
       - Data stays in shared memory, PowerShell holds lightweight wrapper
       - read() returns memoryview over shared memory (no data movement)
       - Export back to Python reuses same channel (instant, no copy)
    
    2. **Copy-based** (traditional, works with all types):
       - publish() with zero_copy=False copies to PowerShell heap
       - fetch() copies data back to Python
       - Slower but supports text/JSON and complex objects
    
    Examples
    --------
    >>> # Zero-copy: publish 75 MiB, read back instantly
    >>> data = os.urandom(75 * 1024 * 1024)
    >>> result = bridge.publish(data, variable="global:Buf", zero_copy=True)
    >>> view = bridge.read(result)
    >>> assert len(view.memoryview) == len(data)
    >>> view.close()
    
    >>> # Copy-based: publish JSON, fetch back
    >>> bridge.publish({"key": "value"}, variable="global:Config", 
    ...                format="json", zero_copy=False)
    >>> config = bridge.fetch("$global:Config", format="json")
    """

    def __init__(
        self,
        shell: Shell,
        *,
        default_frame_bytes: int = 1024 * 1024,
        poll_interval: float = POLL_INTERVAL,
        poll_attempts: int = POLL_ATTEMPTS,
    ) -> None:
        """Initialize shared memory bridge.
        
        Parameters
        ----------
        shell
            Shell instance to communicate with PowerShell
        default_frame_bytes
            Default frame size for fetch operations
        poll_interval
            Polling interval in seconds
        poll_attempts
            Maximum poll attempts before timeout
        """
        self._shell = shell
        self._default_frame_bytes = int(default_frame_bytes)
        self._poll_interval = float(poll_interval)
        self._poll_attempts = int(poll_attempts)
        self._ready = False

        if self._default_frame_bytes <= 0:
            raise ValueError("default_frame_bytes must be positive")
        if self._poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if self._poll_attempts <= 0:
            raise ValueError("poll_attempts must be positive")

    def publish(
        self,
        data: Union[BytesLike, str, Any],
        *,
        variable: str,
        format: Format = "bytes",
        encoding: str = "utf-8",
        frame_bytes: Optional[int] = None,
        channel_name: Optional[str] = None,
        zero_copy: bool = True,
    ) -> PublishResult:
        """Publish data to PowerShell variable.

        Parameters
        ----------
        data
            Data to publish
        variable
            PowerShell variable name (e.g., 'global:MyData')
        format
            Data format: 'bytes', 'text', or 'json'
        encoding
            Text encoding (default: 'utf-8')
        frame_bytes
            Explicit frame size (defaults to data length)
        channel_name
            Explicit channel name (auto-generated if omitted)
        zero_copy
            If True (default), create zero-copy SharedMemoryBuffer in PowerShell.
            If False, copy data to PowerShell heap.

        Returns
        -------
        PublishResult
            Contains channel info. Pass to read() for zero-copy retrieval.
        """
        self._ensure_ready()
        
        # Encode data
        payload = self._encode(data, format, encoding)
        payload_len = len(payload)
        
        if payload_len == 0 and frame_bytes is None:
            raise ValueError("Provide frame_bytes for empty payloads")

        frame_size = int(frame_bytes or payload_len)
        if payload_len > frame_size:
            raise ValueError(f"Payload {payload_len} exceeds frame {frame_size}")

        # Create channel and write
        chan_name = channel_name or f"vsbridge_{uuid.uuid4().hex}"
        channel = _core.SharedMemoryChannel(chan_name, frame_size)
        
        try:
            channel.write_to_powershell(payload)
            seq = int(channel.get_python_seq())
            if seq <= 0:
                raise RuntimeError("Sequence did not advance after write")
            sequence = seq - 1

            # Import in PowerShell
            fmt_token = "ZeroCopy" if zero_copy else self._format_name(format)
            ps_cmd = (
                f"Import-SharedMemoryData "
                f"-ChannelName {_quote_ps(chan_name)} "
                f"-FrameBytes {frame_size} "
                f"-Format {_quote_ps(fmt_token)} "
                f"-Encoding {_quote_ps(encoding)} "
                f"-VariableName {_quote_ps(variable)}"
            )
            self._shell.run(ps_cmd, raise_on_error=True)
            
            return PublishResult(
                channel_name=chan_name,
                frame_bytes=frame_size,
                data_length=payload_len,
                sequence=sequence,
                zero_copy=zero_copy
            )
        finally:
            del channel

    def read(self, result: PublishResult) -> ZeroCopyView:
        """Read zero-copy published data (no data movement).
        
        Parameters
        ----------
        result
            PublishResult from publish() with zero_copy=True
            
        Returns
        -------
        ZeroCopyView
            View with memoryview attribute. Call close() when done.
            
        Notes
        -----
        Data remains in PY→PS region of shared memory. This creates a
        memoryview over that region without copying bytes.
        """
        if not result.zero_copy:
            raise ValueError("read() requires zero_copy=True. Use fetch() instead.")
        
        self._ensure_ready()
        channel = _core.SharedMemoryChannel(result.channel_name, result.frame_bytes)
        
        try:
            # Map entire shared memory region: [header][PY→PS][PS→PY]
            total_size = HEADER_BYTES + 2 * result.frame_bytes
            mm = mmap.mmap(-1, total_size, tagname=result.channel_name, access=mmap.ACCESS_READ)
            
            # PY→PS region starts at HEADER_BYTES
            offset = HEADER_BYTES
            view = memoryview(mm)[offset : offset + result.data_length]
            
            return ZeroCopyView(
                channel_name=result.channel_name,
                memoryview=view,
                length=result.data_length,
                frame_bytes=result.frame_bytes,
                _mmap=mm,
                _channel=channel
            )
        except Exception:
            del channel
            raise

    def pull(
        self,
        variable: str,
        *,
        frame_bytes: Optional[int] = None,
        channel_name: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ZeroCopyView:
        """Pull PowerShell variable to shared memory with zero-copy.
        
        This is the inverse of publish(): PowerShell copies an existing variable
        directly to shared memory via DLL, then Python reads it zero-copy.
        
        Parameters
        ----------
        variable
            PowerShell variable name (e.g., 'global:MyData' or '$MyVar')
        frame_bytes
            Shared memory frame size (must be ≥ variable size)
        channel_name
            Explicit channel name (auto-generated if omitted)
        timeout
            Timeout in seconds (default: 10s for large copies)
            
        Returns
        -------
        ZeroCopyView
            View with memoryview attribute. Call close() when done.
            
        Examples
        --------
        >>> # PowerShell creates large variable
        >>> shell.run("$global:BigData = [byte[]](1..1000000)")
        >>> # Pull to Python with zero-copy
        >>> view = bridge.pull("global:BigData", frame_bytes=1_000_000)
        >>> print(len(view.memoryview))  # 1000000
        >>> view.close()
        """
        self._ensure_ready()
        
        # Parse variable reference
        var_ref = self._extract_var_ref(variable) or variable.lstrip('$')
        
        # Determine frame size (default to 1 MiB if not specified)
        frame_size = int(frame_bytes or 1024 * 1024)
        if frame_size <= 0:
            raise ValueError("frame_bytes must be positive")
        
        # Default timeout for large copies (allow ~1-2s per MiB)
        effective_timeout = timeout or max(30.0, frame_size / (1024 * 1024) * 2.0)
        
        # Create channel
        chan_name = channel_name or f"vsbridge_pull_{uuid.uuid4().hex}"
        channel = _core.SharedMemoryChannel(chan_name, frame_size)
        
        try:
            # Ask PowerShell to copy variable to shared memory
            ps_cmd = (
                f"Copy-VariableToSharedMemory "
                f"-ChannelName {_quote_ps(chan_name)} "
                f"-FrameBytes {frame_size} "
                f"-VariableName {_quote_ps(var_ref)}"
            )
            
            result = self._shell.run(ps_cmd, raise_on_error=True, timeout=effective_timeout)
            
            # Wait for PowerShell to write
            seq = self._wait_for_ps(channel, effective_timeout)
            data_len = int(channel.get_powershell_length())
            
            if data_len > frame_size:
                raise ValueError(f"Variable size {data_len} exceeds frame {frame_size}")
            
            print(f"[Python] Variable copied to shared memory: {data_len} bytes")
            
            # Map shared memory for zero-copy access
            # Data is in PS→PY region at offset: HEADER_BYTES + frame_size
            total_size = HEADER_BYTES + 2 * frame_size
            mm = mmap.mmap(-1, total_size, tagname=chan_name, access=mmap.ACCESS_READ)
            
            # PS→PY region starts at HEADER_BYTES + frame_size
            offset = HEADER_BYTES + frame_size
            view = memoryview(mm)[offset : offset + data_len]
            
            return ZeroCopyView(
                channel_name=chan_name,
                memoryview=view,
                length=data_len,
                frame_bytes=frame_size,
                _mmap=mm,
                _channel=channel
            )
        except Exception:
            del channel
            raise

    def create_buffer(
        self,
        size: int,
        *,
        variable: str,
        channel_name: Optional[str] = None,
        writable: bool = True,
    ) -> memoryview:
        """Create shared memory buffer accessible from PowerShell.
        
        This creates a buffer that PowerShell can write to DIRECTLY without
        intermediate copies. Perfect for PowerShell operations that generate
        large results - they can write directly to shared memory.
        
        Parameters
        ----------
        size
            Buffer size in bytes
        variable
            PowerShell variable name to create (e.g., 'global:Buffer')
        channel_name
            Explicit channel name (auto-generated if omitted)
        writable
            Whether buffer should be writable from Python side
            
        Returns
        -------
        memoryview
            Writable memoryview over PS→PY region. PowerShell can write here.
            
        Examples
        --------
        >>> # Create 10 MiB buffer accessible from PowerShell
        >>> buf = bridge.create_buffer(10*1024*1024, variable="global:OutputBuf")
        >>> # PowerShell can now write directly to this buffer
        >>> shell.run('''
        ...     for ($i = 0; $i -lt $global:OutputBuf.Capacity; $i++) {
        ...         $global:OutputBuf.Write($i, [byte]($i % 256))
        ...     }
        ... ''')
        >>> # Python reads zero-copy
        >>> data = bytes(buf[:100])
        >>> print(list(data))  # [0, 1, 2, ..., 99]
        """
        self._ensure_ready()
        
        if size <= 0:
            raise ValueError("size must be positive")
        
        # Parse variable reference
        var_ref = self._extract_var_ref(variable) or variable.lstrip('$')
        
        # Create channel
        chan_name = channel_name or f"vsbridge_buf_{uuid.uuid4().hex}"
        channel = _core.SharedMemoryChannel(chan_name, size)
        
        try:
            # Create WriteableBuffer in PowerShell that points to PS→PY region
            ps_cmd = (
                f"New-SharedMemoryWriteableBuffer "
                f"-ChannelName {_quote_ps(chan_name)} "
                f"-FrameBytes {size} "
                f"-VariableName {_quote_ps(var_ref)}"
            )
            self._shell.run(ps_cmd, raise_on_error=True)
            
            # Map PS→PY region for Python to read what PowerShell writes
            total_size = HEADER_BYTES + 2 * size
            access = mmap.ACCESS_WRITE if writable else mmap.ACCESS_READ
            mm = mmap.mmap(-1, total_size, tagname=chan_name, access=access)
            
            # PS→PY region starts at HEADER_BYTES + size
            offset = HEADER_BYTES + size
            view = memoryview(mm)[offset : offset + size]
            
            # Store channel and mmap to keep them alive
            if not hasattr(self, '_buffers'):
                self._buffers = {}
            self._buffers[chan_name] = (channel, mm)
            
            return view
            
        except Exception:
            del channel
            raise

    def run_command(
        self,
        command: str,
        *,
        convert_to_str: bool = False,
        variable: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Union[memoryview, str]:
        """Run PowerShell command and capture result directly into shared memory.
        
        This is a convenience wrapper that:
        1. Runs your PowerShell command
        2. Auto-converts result to byte[] (supports ANY type!)
        3. Automatically calculates the exact buffer size needed
        4. Creates a shared memory buffer with perfect size
        5. Writes directly to buffer (no intermediate copy)
        6. Returns bytes with actual data (up to first null byte)
        
        Perfect for one-liner operations. Works with strings, arrays, objects!
        No need to guess buffer sizes - it's calculated automatically!
        
        Parameters
        ----------
        command
            PowerShell command to execute (any return type, auto-converted)
        variable
            Optional PowerShell variable name for the buffer
            (auto-generated if omitted)
        timeout
            Command timeout in seconds
            
        Returns
        -------
        memoryview
            Zero-copy view over the result
            
        Supported Types (Auto-Conversion)
        ---------------------------------
        - byte[]     : Direct copy (no conversion)
        - string     : UTF-8 encoding
        - Array      : Auto-detected! If BaseType is System.Array (object arrays),
                       converts to string using Out-String. Otherwise tries byte[] cast.
        - int[]      : Cast to byte[]
        - Stream     : ToArray()
        - PSObject   : JSON serialization
        - null       : Empty byte array
            
        Examples
        --------
        >>> # String command (auto UTF-8 encoding, auto-sized)
        >>> buf = bridge.run_command_into_buffer(
        ...     "Get-Date -Format 'yyyy-MM-dd'"
        ... )
        >>> date = bytes(buf[:10]).decode('utf-8')
        
        >>> # Integer range (auto byte conversion, auto-sized)
        >>> buf = bridge.run_command_into_buffer("0..255")
        
        >>> # Array of objects (auto-detects System.Array, converts to string)
        >>> buf = bridge.run_command_into_buffer(
        ...     "Get-ChildItem $env:WINDIR\\System32 | Select Name, Length"
        ... )
        
        >>> # Large data - buffer size calculated automatically!
        >>> buf = bridge.run_command_into_buffer(
        ...     "$big = Get-ChildItem -Recurse; $big",
        ...     timeout=120.0
        ... )
        
        >>> # Crypto random bytes (already byte[], auto-sized)
        >>> buf = bridge.run_command_into_buffer(
        ...     "$rng = [Security.Cryptography.RNGCryptoServiceProvider]::new(); "
        ...     "$bytes = [byte[]]::new(1048576); "
        ...     "$rng.GetBytes($bytes); "
        ...     "$rng.Dispose(); "
        ...     "$bytes"
        ... )
        """
        self._ensure_ready()
        

        
        # Build PowerShell command that assigns result to buffer
        # Auto-convert common types to byte[]
        ps_script = f"""
            $__result = {command}
            
            # Auto-convert to byte[] if needed
            if ($__result -is [byte[]]) {{
                $__bytes = $__result
            }}
            elseif ($__result -is [string]) {{
                $__bytes = [System.Text.Encoding]::UTF8.GetBytes($__result)
            }}
            elseif ($__result -is [System.IO.Stream]) {{
                $__ms = [System.IO.MemoryStream]::new()
                $__result.CopyTo($__ms)
                $__bytes = $__ms.ToArray()
                $__ms.Dispose()
            }}
            elseif ($__result -is [System.Array]) {{
                # Check if BaseType is System.Array (object arrays, not primitive byte arrays)
                $__baseType = $__result.GetType().BaseType
                if ($__baseType -and $__baseType.FullName -eq 'System.Array') {{
                    # Convert array to string using Out-String
                    $__str = $__result | Out-String
                    $__bytes = [System.Text.Encoding]::UTF8.GetBytes($__str)
                }} else {{
                    # Try direct cast for primitive arrays
                    try {{
                        $__bytes = [byte[]]$__result
                    }} catch {{
                        # Manual conversion for non-byte arrays
                        $__list = [System.Collections.Generic.List[byte]]::new()
                        foreach ($__item in $__result) {{
                            $__list.Add([byte]$__item)
                        }}
                        $__bytes = $__list.ToArray()
                    }}
                }}
            }}
            elseif ($null -eq $__result) {{
                $__bytes = [byte[]]::new(0)
            }}
            else {{
                # Fallback: JSON serialize
                $__json = ConvertTo-Json -InputObject $__result -Depth 64 -Compress
                $__bytes = [System.Text.Encoding]::UTF8.GetBytes($__json)
            }}
            [System.Text.Encoding]::UTF8.GetByteCount(($__bytes))

        """
        
        # Execute command
        result = self._shell.run(ps_script, raise_on_error=True, timeout=timeout)

        exact_size = int(result.out.strip())
        # Generate variable name if not provided
        var_name = variable or f"global:__vshell_buf_{uuid.uuid4().hex[:8]}"
        
        # Create buffer
        buf = self.create_buffer(exact_size, variable=var_name)
        
        # Parse variable for PowerShell reference
        var_ref = self._extract_var_ref(var_name) or var_name.lstrip('$')
        self._shell.run(f'${var_ref}.WriteBytes($__bytes)', raise_on_error=True, timeout=timeout)
        if not convert_to_str:
            return buf
        data = buf.tobytes()
        null_pos = data.find(b'\x00')
        if null_pos > 0:
            text = data[:null_pos].decode('utf-8', errors='replace')
        else:
            text = data.decode('utf-8', errors='replace')
        return text


    def fetch(
        self,
        command: str,
        *,
        format: Format = "json",
        encoding: str = "utf-8",
        frame_bytes: Optional[int] = None,
        channel_name: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Execute PowerShell and copy result to Python.

        Parameters
        ----------
        command
            PowerShell expression to evaluate
        format
            Result format: 'bytes', 'text', or 'json'
        encoding
            Text encoding
        frame_bytes
            Frame size (defaults to default_frame_bytes)
        channel_name
            Explicit channel name
        timeout
            Timeout in seconds

        Returns
        -------
        bytes, str, or Any
            Decoded result based on format
        """
        self._ensure_ready()
        
        frame_size = int(frame_bytes or self._default_frame_bytes)
        if frame_size <= 0:
            raise ValueError("frame_bytes must be positive")
        
        chan_name = channel_name or f"vsbridge_{uuid.uuid4().hex}"
        channel = _core.SharedMemoryChannel(chan_name, frame_size)
        
        try:
            # Check if command is a simple variable reference
            var_ref = self._extract_var_ref(command)
            
            if var_ref:
                # Use optimized variable export
                ps_cmd = (
                    f"Export-SharedMemoryVarBytes "
                    f"-ChannelName {_quote_ps(chan_name)} "
                    f"-FrameBytes {frame_size} "
                    f"-VariableName {_quote_ps(var_ref)} "
                    f"-Encoding {_quote_ps(encoding)}"
                )
            else:
                # Use general export
                fmt_token = self._format_name(format)
                ps_cmd = (
                    f"Export-SharedMemoryData "
                    f"-ChannelName {_quote_ps(chan_name)} "
                    f"-FrameBytes {frame_size} "
                    f"-Format {_quote_ps(fmt_token)} "
                    f"-Encoding {_quote_ps(encoding)} "
                    f"-Command {_quote_ps(command)}"
                )
            
            self._shell.run(ps_cmd, raise_on_error=True, timeout=timeout)
            
            # Wait for PowerShell to write
            seq = self._wait_for_ps(channel, timeout)
            data_len = int(channel.get_powershell_length())
            
            if data_len > frame_size:
                raise ValueError(f"Data {data_len} exceeds frame {frame_size}")

            # Read and decode
            raw = channel.read_from_powershell(seq - 1)
            payload = raw[:data_len] if data_len < len(raw) else raw
            return self._decode(payload, format, encoding)
        finally:
            del channel

    # Internal helpers

    def _ensure_ready(self) -> None:
        """Ensure PowerShell scripts are loaded."""
        if self._ready:
            return
        
        if not self._shell.is_running:
            self._shell.start()
        
        mod_dir = _module_dir()
        
        # Load shared_memory.ps1
        if not getattr(self._shell, "_vshell_shm_loaded", False):
            self._shell.script(mod_dir / "shared_memory.ps1", dot_source=True)
            setattr(self._shell, "_vshell_shm_loaded", True)
        
        # Load shared_memory_bridge.ps1
        if not getattr(self._shell, "_vshell_shm_bridge_loaded", False):
            dll_dir = mod_dir / "win_pwsh_dll"
            self._shell.script(
                mod_dir / "shared_memory_bridge.ps1",
                dot_source=True,
                args=[str(dll_dir)]
            )
            setattr(self._shell, "_vshell_shm_bridge_loaded", True)
        
        self._ready = True

    def _encode(self, data: Any, format: Format, encoding: str) -> bytes:
        """Encode data to bytes based on format."""
        if format == "bytes":
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError(f"format='bytes' requires bytes-like, got {type(data).__name__}")
            return _as_bytes(data)
        if format == "text":
            return str(data).encode(encoding)
        if format == "json":
            return json.dumps(data).encode(encoding)
        raise ValueError(f"Unsupported format: {format}")

    def _decode(self, payload: bytes, format: Format, encoding: str) -> Any:
        """Decode bytes based on format."""
        if format == "bytes":
            return payload
        if format == "text":
            return payload.decode(encoding)
        if format == "json":
            return json.loads(payload.decode(encoding)) if payload else None
        raise ValueError(f"Unsupported format: {format}")

    def _format_name(self, format: Format) -> str:
        """Get PowerShell format token."""
        return {"bytes": "Bytes", "text": "String", "json": "Json"}[format]

    def _wait_for_ps(self, channel: Any, timeout: Optional[float]) -> int:
        """Poll until PowerShell writes data."""
        attempts = self._poll_attempts
        if timeout and timeout > 0:
            attempts = max(attempts, int(math.ceil(timeout / self._poll_interval)))

        for _ in range(attempts):
            seq = channel.get_powershell_seq()
            if seq > 0:
                return int(seq)
            time.sleep(self._poll_interval)
        
        raise TimeoutError("Timed out waiting for PowerShell")

    def _extract_var_ref(self, cmd: str) -> Optional[str]:
        """Extract variable reference from command like '$global:Var'."""
        m = VAR_REF_RE.match(cmd)
        if not m:
            return None
        scope = m.group("scope") or ""
        name = m.group("name")
        return f"{scope}{name}"
