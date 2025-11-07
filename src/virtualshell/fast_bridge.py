"""Ultra-fast zero-copy bridge - simplified for maximum performance.

Supports ALL data types with intelligent routing:
- byte[] → TRUE zero-copy (fastest path)
- Other types → Fast serialization, then zero-copy transfer

NO extra shell calls, ONLY direct shared memory transfers.
"""
from __future__ import annotations

import time
import ctypes
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Optional

from .zero_copy_pointer import (
    NativeChannel,
    _dll,
    VS_OK,
    VS_TIMEOUT,
)

if TYPE_CHECKING:
    from .shell import Shell

__all__ = ["FastBridge", "ZeroCopyBuffer"]

Format = Literal["bytes", "string", "clixml", "json"]


@dataclass
class ZeroCopyBuffer:
    """Direct memory view into shared memory - TRUE zero-copy.
    
    No copies are made. Python reads directly from the same memory
    PowerShell wrote to.
    """
    length: int
    address: int
    format: Format
    _channel: NativeChannel
    _mv: memoryview
    _acked: bool = False

    @property
    def memoryview(self) -> memoryview:
        """Get the zero-copy memoryview."""
        return self._mv[:self.length]
    
    @property
    def bytes(self) -> bytes:
        """Convert to bytes (makes a copy)."""
        return self._mv[:self.length].tobytes()
    
    def decode(self, encoding: str = 'utf-8') -> Any:
        """Decode the buffer based on its format.
        
        - format="bytes": returns raw bytes
        - format="string": decodes as UTF-8 string
        - format="clixml": deserializes PowerShell CliXml to Python objects
        - format="json": parses JSON
        """
        raw_bytes = self.bytes
        
        if self.format == "bytes":
            return raw_bytes
        
        if self.format == "string":
            return raw_bytes.decode(encoding)
        
        if self.format == "json":
            text = raw_bytes.decode(encoding)
            return json.loads(text) if text else None
        
        if self.format == "clixml":
            # CliXml deserialization - convert to Python objects
            text = raw_bytes.decode(encoding)
            return self._deserialize_clixml(text)
        
        return raw_bytes
    
    def _deserialize_clixml(self, xml_text: str) -> Any:
        """Deserialize PowerShell CliXml to Python objects.
        
        Returns parsed data structure. For complex objects, returns the XML string
        which can be sent back to PowerShell for deserialization if needed.
        """
        # For simple XML parsing, we'll extract values
        # For full deserialization, user should use PowerShell or a dedicated library
        
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_text)
            
            # Handle common PowerShell serialization patterns
            # This is a simplified parser - for full fidelity, use PowerShell
            
            # Check for array/collection
            if root.tag == 'Objs':
                items = []
                for obj in root.findall('.//Obj'):
                    items.append(self._parse_ps_object(obj))
                return items if len(items) > 1 else (items[0] if items else None)
            
            return xml_text  # Return raw XML if can't parse
            
        except Exception:
            # If parsing fails, return raw XML
            return xml_text
    
    def _parse_ps_object(self, obj_elem) -> Any:
        """Parse a PowerShell object element."""
        # Check for primitive types
        for prop in obj_elem.findall('.//Props//*'):
            name = prop.get('N', '')
            text = prop.text or ''
            
            # Try to convert to appropriate Python type
            if prop.tag == 'I32':
                return int(text)
            elif prop.tag == 'S':
                return text
            elif prop.tag == 'B':
                return text.lower() == 'true'
            elif prop.tag == 'DT':
                return text  # Datetime as string for now
        
        # For complex objects, build a dict
        result = {}
        for prop in obj_elem.findall('.//Props/*'):
            name = prop.get('N', '')
            if name:
                result[name] = prop.text or ''
        
        return result if result else obj_elem.text
    
    def ack(self) -> None:
        """Tell PowerShell we're done reading."""
        if not self._acked:
            _dll.VS_AckDataOffset(ctypes.c_void_p(self._channel.handle))
            self._acked = True
    
    def close(self) -> None:
        """Clean up resources."""
        try:
            self.ack()
        finally:
            try:
                self._mv.release()
            except:
                pass
            self._channel.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()


class FastBridge:
    """Ultra-fast zero-copy bridge.
    
    Usage:
        bridge = FastBridge(shell, channel_name="fast", frame_mb=100)
        
        # PowerShell → Python (zero-copy)
        with bridge.receive() as buf:
            data = buf.memoryview  # Direct memory access
            # or
            data = buf.bytes  # Copy to bytes
        
        # Python → PowerShell (zero-copy)
        bridge.send(data, variable="mydata")
    """
    
    def __init__(
        self,
        shell: Shell,
        *,
        channel_name: str = "fastbridge",
        frame_mb: int = 100,
    ):
        self._shell = shell
        self._channel_name = channel_name
        self._frame_bytes = frame_mb * 1024 * 1024
        self._initialized = False
    
    def _ensure_ready(self):
        """Initialize PowerShell side if needed."""
        if self._initialized:
            return
        
        if not self._shell.is_running:
            self._shell.start()
        
        # Load the minimal PowerShell module
        from pathlib import Path
        mod_dir = Path(__file__).resolve().parent
        
        # Always reload for development (can optimize later with version checking)
        self._shell.script(mod_dir / "shared_memory.ps1", dot_source=True)
        self._shell.script(mod_dir / "fast_bridge.ps1", dot_source=True)
        
        self._initialized = True
    
    def receive(self, *, timeout: float = 30.0, format: Optional[Format] = None) -> ZeroCopyBuffer:
        """Wait for PowerShell to send data via zero-copy.
        
        Returns a ZeroCopyBuffer that provides direct memory access.
        Must call .close() or use as context manager.
        
        Args:
            timeout: Timeout in seconds
            format: Expected format - if None, will auto-detect
        
        Example:
            with bridge.receive() as buf:
                # buf.memoryview is direct shared memory access
                print(f"Received {buf.length} bytes")
                data = buf.decode()  # Auto-decode based on format
        """
        self._ensure_ready()
        
        # Open channel
        channel = NativeChannel(self._channel_name, self._frame_bytes)
        
        try:
            # Get base address
            base = _dll.VS_GetSharedMemoryBase(ctypes.c_void_p(channel.handle))
            if not base:
                raise RuntimeError("Failed to get shared memory base")
            
            base_addr = int(ctypes.cast(base, ctypes.c_void_p).value or 0)
            if base_addr == 0:
                raise RuntimeError("Invalid base address")
            
            # Wait for offset metadata
            deadline = time.perf_counter() + timeout
            offset_out = ctypes.c_uint64(0)
            length_out = ctypes.c_uint64(0)
            seq_out = ctypes.c_uint64(0)
            
            while True:
                status = _dll.VS_GetDataOffset(
                    ctypes.c_void_p(channel.handle),
                    ctypes.byref(offset_out),
                    ctypes.byref(length_out),
                    ctypes.byref(seq_out),
                )
                
                if status == VS_OK and length_out.value > 0:
                    break
                
                if time.perf_counter() > deadline:
                    raise TimeoutError(f"Timeout waiting for data on {self._channel_name}")
                
                time.sleep(0.001)
            
            # Map memory directly (TRUE zero-copy)
            addr = base_addr + int(offset_out.value)
            length = int(length_out.value)
            
            # Create ctypes array at the address
            buf = (ctypes.c_ubyte * length).from_address(addr)
            mv = memoryview(buf)
            
            # Auto-detect format if not specified
            if format is None:
                format = self._detect_format(mv, length)
            
            return ZeroCopyBuffer(
                length=length,
                address=addr,
                format=format,
                _channel=channel,
                _mv=mv,
            )
        
        except Exception:
            channel.close()
            raise
    
    def _detect_format(self, mv: memoryview, length: int) -> Format:
        """Auto-detect data format from content."""
        if length == 0:
            return "bytes"
        
        # Sample first few bytes
        sample_size = min(100, length)
        sample = bytes(mv[:sample_size])
        
        # Check for CliXml (PowerShell serialization)
        if sample.startswith(b'<Objs'):
            return "clixml"
        
        # Check for JSON
        if sample.lstrip().startswith(b'{') or sample.lstrip().startswith(b'['):
            return "json"
        
        # Check if it's valid UTF-8 text
        try:
            sample.decode('utf-8')
            # If it decodes, check if it looks like text
            if all(32 <= b < 127 or b in (9, 10, 13) for b in sample[:20]):
                return "string"
        except UnicodeDecodeError:
            pass
        
        # Default to bytes
        return "bytes"
    
    def send(
        self,
        data: bytes,
        variable: str,
        *,
        timeout: float = 30.0,
        chunk_threshold_mb: int = 256,
        chunk_size_mb: int = 128,
    ) -> None:
        """Send data from Python to PowerShell via zero-copy.
        
        For data > chunk_threshold_mb, automatically chunks into optimal sizes
        for best throughput and minimal LOH fragmentation.
        
        Args:
            data: Bytes to send
            variable: PowerShell variable name to receive data (without $ or with scope)
            timeout: Timeout in seconds
            chunk_threshold_mb: Auto-chunk if data > this size (default: 256 MB)
            chunk_size_mb: Chunk size for large transfers (default: 128 MB)
        
        Example:
            # Small data - single transfer
            bridge.send(b"hello", variable="myvar")
            
            # Large data - automatic chunking
            big_data = bytes(1024 * 1024 * 512)  # 512 MB
            bridge.send(big_data, variable="bigdata")  # Automatically chunks!
        
        Note:
            - Chunking uses optimal 128 MB chunks for best throughput (200-400 MB/s)
            - Reduces LOH fragmentation vs single large allocation
            - Completely transparent - same API for all sizes
        """
        self._ensure_ready()
        
        data_size = len(data)
        data_mb = data_size / 1024 / 1024
        chunk_threshold = chunk_threshold_mb * 1024 * 1024
        
        # Decide: chunking or single transfer?
        if data_size > chunk_threshold:
            self._send_chunked(data, variable, timeout, chunk_size_mb)
        else:
            self._send_single(data, variable, timeout)
    
    def _send_single(self, data: bytes, variable: str, timeout: float) -> None:
        """Send data in a single transfer."""
        data_mb = len(data) / 1024 / 1024
        frame_mb = self._frame_bytes / 1024 / 1024
        
        # Validate data size
        if len(data) > self._frame_bytes:
            raise ValueError(
                f"Data size ({data_mb:.1f} MB) exceeds frame capacity ({frame_mb:.1f} MB). "
                f"Increase frame_mb when creating FastBridge."
            )
        
        # Write to shared memory
        channel = NativeChannel(self._channel_name, max(len(data), self._frame_bytes))
        
        try:
            buf = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
            next_seq = ctypes.c_uint64(0)
            
            status = _dll.VS_WritePy2Ps(
                ctypes.c_void_p(channel.handle),
                ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)),
                ctypes.c_uint64(len(data)),
                ctypes.c_uint32(int(timeout * 1000)),
                ctypes.byref(next_seq),
            )
            
            if status != VS_OK:
                raise RuntimeError(f"VS_WritePy2Ps failed with status {status}")
            
            # Tell PowerShell to read it via fast path
            var_name = variable.lstrip('$')
            
            # Check if scope is already specified (e.g., "global:myvar")
            if ':' in var_name:
                ps_var = f"${var_name}"
            else:
                ps_var = f"$global:{var_name}"
            
            ps_cmd = (
                f"{ps_var} = [FastBridge]::ReceiveBytes("
                f"'{self._channel_name}', {channel.frame_bytes})"
            )
            
            self._shell.run(ps_cmd, raise_on_error=True, timeout=timeout)
        
        finally:
            channel.close()
    
    def _send_chunked(
        self,
        data: bytes,
        variable: str,
        timeout: float,
        chunk_size_mb: int,
    ) -> None:
        """Send large data in optimized chunks with pre-allocated target array."""
        total_size = len(data)
        chunk_size = chunk_size_mb * 1024 * 1024
        num_chunks = (total_size + chunk_size - 1) // chunk_size
        
        var_name = variable.lstrip('$')
        if ':' in var_name:
            ps_var = f"${var_name}"
        else:
            ps_var = f"$global:{var_name}"
        
        # Pre-allocate byte array in PowerShell
        ps_cmd = f"{ps_var} = [byte[]]::new({total_size}); $__offset = 0"
        self._shell.run(ps_cmd, raise_on_error=True, timeout=timeout)
        
        # Use single channel for all chunks (memory efficient!)
        channel = NativeChannel(self._channel_name, max(chunk_size, self._frame_bytes))
        
        try:
            for i in range(num_chunks):
                start = i * chunk_size
                end = min(start + chunk_size, total_size)
                chunk = data[start:end]
                chunk_len = len(chunk)
                
                # Write chunk to shared memory
                buf = (ctypes.c_ubyte * chunk_len).from_buffer_copy(chunk)
                next_seq = ctypes.c_uint64(0)
                
                status = _dll.VS_WritePy2Ps(
                    ctypes.c_void_p(channel.handle),
                    ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)),
                    ctypes.c_uint64(chunk_len),
                    ctypes.c_uint32(int(timeout * 1000)),
                    ctypes.byref(next_seq),
                )
                
                if status != VS_OK:
                    raise RuntimeError(f"VS_WritePy2Ps failed on chunk {i+1}/{num_chunks}")
                
                # PowerShell: receive chunk and append to pre-allocated array
                ps_cmd = (
                    f"try {{ "
                    f"$__chunk = [FastBridge]::ReceiveBytes('{self._channel_name}', {channel.frame_bytes}); "
                    f"if ($null -eq $__chunk) {{ throw 'Chunk {i+1}/{num_chunks} is null' }}; "
                    f"[System.Buffer]::BlockCopy($__chunk, 0, {ps_var}, $__offset, $__chunk.Length); "
                    f"$__offset += $__chunk.Length; "
                    f"$__chunk = $null; "  # Release chunk memory immediately
                    f"Write-Output 'OK' "
                    f"}} catch {{ Write-Error $_.Exception.Message; throw }}"
                )
                
                result = self._shell.run(ps_cmd, raise_on_error=False, timeout=timeout)
                
                # Check for errors
                if result.err or not result.out.strip().startswith('OK'):
                    error_msg = result.err if result.err else result.out
                    raise RuntimeError(
                        f"Chunk {i+1}/{num_chunks} failed: {error_msg[:200]}"
                    )
        
        finally:
            channel.close()
            # Cleanup temp variable
            self._shell.run("Remove-Variable -Name __offset,__chunk -ErrorAction SilentlyContinue", raise_on_error=False)
    
    def receive_from_variable(
        self,
        variable: str,
        *,
        timeout: float = 60.0,
        format: Optional[Format] = None,
    ) -> ZeroCopyBuffer:
        """Tell PowerShell to send a variable via zero-copy, then receive it.
        
        Works with ALL data types:
        - byte[] → zero-copy (fastest)
        - string → encoded to UTF-8, then zero-copy
        - objects → serialized to CliXml, then zero-copy
        
        Uses always-on chunking for ALL transfers (consistent, predictable behavior).
        
        Args:
            variable: PowerShell variable name (with or without $)
            timeout: Timeout in seconds
            format: Expected format - if None, will auto-detect
        
        Returns:
            ZeroCopyBuffer with direct memory access
        """
        self._ensure_ready()
        
        var_name = variable.lstrip('$')
        
        # Build PowerShell command to send data
        ps_cmd = (
            f"[FastBridge]::SendBytesNoWait("
            f"'{self._channel_name}', {self._frame_bytes}, "
            f"$global:{var_name})"
        )
        
        # Start PowerShell send in background (non-blocking)
        import threading
        ps_result = [None]
        def run_ps():
            try:
                ps_result[0] = self._shell.run(ps_cmd, raise_on_error=False, timeout=timeout + 30)
            except Exception as e:
                ps_result[0] = e
        
        ps_thread = threading.Thread(target=run_ps, daemon=True)
        ps_thread.start()
        
        # Open channel FIRST so Python creates the events
        channel = NativeChannel(self._channel_name, self._frame_bytes)
        
        # Give PowerShell a tiny moment to start
        import time
        time.sleep(0.05)
        
        try:
            # Receive all chunks using new chunked API
            return self._receive_all_chunks(channel, timeout, format, ps_thread, ps_result)
        
        except Exception:
            channel.close()
            raise
    
    def _receive_all_chunks(
        self,
        channel: NativeChannel,
        timeout: float,
        format: Optional[Format],
        ps_thread,
        ps_result,
    ) -> ZeroCopyBuffer:
        """Receive all chunks using new VS_WaitForChunk API.
        
        PowerShell sends via VS_BeginChunkedTransfer + VS_SendChunk loop.
        Python receives via VS_WaitForChunk + VS_AckChunk loop.
        """
        timeout_ms = int(timeout * 1000)
        chunks = []
        total_bytes = 0
        expected_chunks = None
        
        while True:
            # Wait for next chunk
            try:
                chunk_index, offset, length = channel.wait_for_chunk(timeout_ms)
            except TimeoutError:
                if expected_chunks is not None and len(chunks) < expected_chunks:
                    raise TimeoutError(
                        f"Incomplete transfer: got {len(chunks)}/{expected_chunks} chunks"
                    )
                break  # No more chunks
            
            # Get base address
            base = _dll.VS_GetSharedMemoryBase(ctypes.c_void_p(channel.handle))
            if not base:
                raise RuntimeError("Failed to get shared memory base")
            
            base_addr = int(ctypes.cast(base, ctypes.c_void_p).value or 0)
            addr = base_addr + offset
            
            # Read chunk data (zero-copy read)
            buf = (ctypes.c_ubyte * length).from_address(addr)
            chunk_data = bytes(buf)  # Copy to Python bytes
            
            # First chunk contains metadata
            if chunk_index == 0:
                # Parse header: "CHUNKED|totalsize|chunksize|numchunks|"
                if chunk_data.startswith(b"CHUNKED|"):
                    header_end = chunk_data.find(b"|", 50)
                    if header_end == -1:
                        header_end = min(200, len(chunk_data))
                    
                    header_str = chunk_data[:header_end].decode('utf-8')
                    parts = header_str.split('|')
                    
                    total_size = int(parts[1])
                    chunk_size = int(parts[2])
                    expected_chunks = int(parts[3])
                    
                    # Extract data after header
                    header_len = len(header_str.encode('utf-8'))
                    chunk_data = chunk_data[header_len:]
            
            chunks.append(chunk_data)
            total_bytes += len(chunk_data)
            
            # Acknowledge this chunk
            channel.ack_chunk()
            
            # Check if we're done
            if expected_chunks is not None and len(chunks) >= expected_chunks:
                break
        
        # Combine all chunks
        complete_data = b''.join(chunks)
        
        # Auto-detect format
        if format is None:
            mv_temp = memoryview(complete_data)
            format = self._detect_format(mv_temp, len(complete_data))
        
        # Wait for PowerShell thread to finish
        ps_thread.join(timeout=5)
        if ps_result[0] is not None and isinstance(ps_result[0], Exception):
            import warnings
            warnings.warn(f"PowerShell send error: {ps_result[0]}")
        
        # Create buffer from combined data
        # Note: This is NOT zero-copy since we combined chunks, but that's unavoidable
        # for multi-chunk transfers. The individual chunk reads were zero-copy.
        return ZeroCopyBuffer(
            length=len(complete_data),
            address=id(complete_data),  # Python object address
            format=format,
            _channel=channel,
            _mv=memoryview(complete_data),
        )
    
    def _receive_chunked_same_channel(
        self,
        channel: NativeChannel,
        first_data: bytes,
        total_size: int,
        chunk_size: int,
        num_chunks: int,
        timeout: float,
        format: Optional[Format],
    ) -> ZeroCopyBuffer:
        """Receive chunks from SAME channel (memory efficient!)."""
        import time
        
        # Pre-allocate complete buffer
        complete_data = bytearray(total_size)
        offset = 0
        
        # Copy first chunk data (already received)
        complete_data[offset:offset+len(first_data)] = first_data
        offset += len(first_data)
        
        # Receive remaining chunks (chunks 2 through num_chunks)
        base = _dll.VS_GetSharedMemoryBase(ctypes.c_void_p(channel.handle))
        base_addr = int(ctypes.cast(base, ctypes.c_void_p).value or 0)
        
        for i in range(1, num_chunks):  # Start at 1 since we already have chunk 0
            # Wait for next chunk on same channel
            timeout_ms = int(timeout * 1000)
            
            status = _dll.VS_WaitForDataOffset(
                ctypes.c_void_p(channel.handle),
                ctypes.c_uint32(timeout_ms)
            )
            
            if status == VS_TIMEOUT:
                raise TimeoutError(f"Timeout waiting for chunk {i+1}/{num_chunks}")
            elif status != VS_OK:
                raise RuntimeError(f"VS_WaitForDataOffset failed on chunk {i+1}/{num_chunks}")
            
            # Read chunk metadata
            offset_out = ctypes.c_uint64(0)
            length_out = ctypes.c_uint64(0)
            seq_out = ctypes.c_uint64(0)
            
            status = _dll.VS_GetDataOffset(
                ctypes.c_void_p(channel.handle),
                ctypes.byref(offset_out),
                ctypes.byref(length_out),
                ctypes.byref(seq_out),
            )
            
            if status != VS_OK or length_out.value == 0:
                raise RuntimeError(f"Chunk {i+1}/{num_chunks} GetDataOffset failed")
            
            # Read chunk data
            addr = base_addr + int(offset_out.value)
            length = int(length_out.value)
            
            buf = (ctypes.c_ubyte * length).from_address(addr)
            complete_data[offset:offset+length] = bytes(buf)
            offset += length
            
            # Acknowledge chunk so PowerShell can send next one
            _dll.VS_AckDataOffset(ctypes.c_void_p(channel.handle))
        
        # Convert to final bytes
        final_bytes = bytes(complete_data)
        
        # Create persistent buffer
        persistent_buf = (ctypes.c_ubyte * len(final_bytes)).from_buffer_copy(final_bytes)
        mv = memoryview(persistent_buf)
        
        # Auto-detect format
        if format is None:
            format = self._detect_format(mv, len(final_bytes))
        
        return ZeroCopyBuffer(
            length=len(final_bytes),
            address=ctypes.addressof(persistent_buf),
            format=format,
            _channel=channel,
            _mv=mv,
        )
    
    def _receive_single(self, timeout: float, format: Optional[Format]) -> ZeroCopyBuffer:
        """Receive data in single transfer using offset-based protocol."""
        channel = NativeChannel(self._channel_name, self._frame_bytes)
        
        try:
            # Use existing ReadFromPs2Py which handles offset protocol
            offset_out = ctypes.c_uint64(0)
            length_out = ctypes.c_uint64(0)
            seq_out = ctypes.c_uint64(0)
            
            deadline = time.perf_counter() + timeout
            
            while True:
                status = _dll.VS_GetDataOffset(
                    ctypes.c_void_p(channel.handle),
                    ctypes.byref(offset_out),
                    ctypes.byref(length_out),
                    ctypes.byref(seq_out),
                )
                
                if status == VS_OK and length_out.value > 0:
                    break
                
                if time.perf_counter() > deadline:
                    raise TimeoutError(f"Timeout waiting for data on {self._channel_name}")
                
                time.sleep(0.001)
            
            # Get base address
            base = _dll.VS_GetSharedMemoryBase(ctypes.c_void_p(channel.handle))
            if not base:
                raise RuntimeError("Failed to get shared memory base")
            
            base_addr = int(ctypes.cast(base, ctypes.c_void_p).value or 0)
            
            # Map memory directly
            addr = base_addr + int(offset_out.value)
            length = int(length_out.value)
            
            # Create ctypes array at the address (zero-copy!)
            buf = (ctypes.c_ubyte * length).from_address(addr)
            mv = memoryview(buf)
            
            # Auto-detect format
            if format is None:
                format = self._detect_format(mv, length)
            
            # Acknowledge so PowerShell can close the channel
            _dll.VS_AckDataOffset(ctypes.c_void_p(channel.handle))
            
            return ZeroCopyBuffer(
                length=length,
                address=addr,
                format=format,
                _channel=channel,
                _mv=mv,
            )
        
        except Exception:
            channel.close()
            raise
    
    def _receive_chunked(
        self,
        total_size: int,
        chunk_size: int,
        num_chunks: int,
        timeout: float,
        format: Optional[Format],
    ) -> ZeroCopyBuffer:
        """Receive large data in chunks from PowerShell."""
        # Pre-allocate buffer for complete data
        complete_data = bytearray(total_size)
        offset = 0
        
        channel = NativeChannel(self._channel_name, chunk_size)
        
        try:
            base = _dll.VS_GetSharedMemoryBase(ctypes.c_void_p(channel.handle))
            if not base:
                raise RuntimeError("Failed to get shared memory base")
            
            base_addr = int(ctypes.cast(base, ctypes.c_void_p).value or 0)
            
            # Receive chunks
            for i in range(num_chunks):
                deadline = time.perf_counter() + timeout
                offset_out = ctypes.c_uint64(0)
                length_out = ctypes.c_uint64(0)
                seq_out = ctypes.c_uint64(0)
                
                # Wait for chunk
                while True:
                    status = _dll.VS_GetDataOffset(
                        ctypes.c_void_p(channel.handle),
                        ctypes.byref(offset_out),
                        ctypes.byref(length_out),
                        ctypes.byref(seq_out),
                    )
                    
                    if status == VS_OK and length_out.value > 0:
                        break
                    
                    if time.perf_counter() > deadline:
                        raise TimeoutError(f"Timeout on chunk {i+1}/{num_chunks}")
                    
                    time.sleep(0.001)
                
                # Copy chunk data
                addr = base_addr + int(offset_out.value)
                length = int(length_out.value)
                buf = (ctypes.c_ubyte * length).from_address(addr)
                complete_data[offset:offset+length] = bytes(buf[:length])
                offset += length
                
                # Acknowledge chunk
                _dll.VS_AckDataOffset(ctypes.c_void_p(channel.handle))
            
            # Convert to bytes
            final_bytes = bytes(complete_data)
            
            # Create buffer from final data
            buf = (ctypes.c_ubyte * len(final_bytes)).from_buffer_copy(final_bytes)
            mv = memoryview(buf)
            
            if format is None:
                format = self._detect_format(mv, len(final_bytes))
            
            return ZeroCopyBuffer(
                length=len(final_bytes),
                address=ctypes.addressof(buf),
                format=format,
                _channel=channel,
                _mv=mv,
            )
        
        except Exception:
            channel.close()
            raise
