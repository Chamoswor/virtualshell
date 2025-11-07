"""Zero-copy pointer API for direct PowerShell memory access.

This module provides Python wrappers for the native pointer API that allows
Python to read directly from PowerShell's pinned managed arrays without memcpy.

SAFETY WARNING: This uses raw pointers across process boundaries. The PowerShell
side MUST keep the array pinned until Python acknowledges completion.
"""

import ctypes
from pathlib import Path
from typing import Optional, Tuple


def _get_dll_path() -> Path:
    """Get path to win_pwsh.dll."""
    return Path(__file__).resolve().parent / "win_pwsh.dll"


# Load DLL
_dll = ctypes.CDLL(str(_get_dll_path()))

# VS_Status codes
VS_OK = 0
VS_TIMEOUT = 1
VS_WOULD_BLOCK = 2
VS_SMALL_BUFFER = 3
VS_ERR_INVALID_ARG = -1
VS_ERR_SYS_ERROR = -2
VS_ERR_BAD_STATE = -3
VS_ERR_BAD_CHANNEL = -4
VS_ERR_NO_DATA = -5

# Define API functions

# VS_Channel VS_OpenChannel(const wchar_t* name, uint64_t frame_bytes, uint32_t num_slots, int32_t use_global_fallback)
_dll.VS_OpenChannel.argtypes = [ctypes.c_wchar_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int32]
_dll.VS_OpenChannel.restype = ctypes.c_void_p

# void VS_CloseChannel(VS_Channel ch)
_dll.VS_CloseChannel.argtypes = [ctypes.c_void_p]
_dll.VS_CloseChannel.restype = None

# int32_t VS_SetDataOffset(VS_Channel ch, uint64_t byte_offset, uint64_t byte_length)
_dll.VS_SetDataOffset.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint64]
_dll.VS_SetDataOffset.restype = ctypes.c_int32

# int32_t VS_GetDataOffset(VS_Channel ch, uint64_t* out_offset, uint64_t* out_len, uint64_t* out_seq)
_dll.VS_GetDataOffset.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_uint64)
]
_dll.VS_GetDataOffset.restype = ctypes.c_int32

# int32_t VS_AckDataOffset(VS_Channel ch)
_dll.VS_AckDataOffset.argtypes = [ctypes.c_void_p]
_dll.VS_AckDataOffset.restype = ctypes.c_int32

# int32_t VS_WaitForDataOffset(VS_Channel ch, uint32_t timeout_ms)
_dll.VS_WaitForDataOffset.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
_dll.VS_WaitForDataOffset.restype = ctypes.c_int32

# int32_t VS_WaitForDataOffsetAck(VS_Channel ch, uint32_t timeout_ms)
_dll.VS_WaitForDataOffsetAck.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
_dll.VS_WaitForDataOffsetAck.restype = ctypes.c_int32

# int32_t VS_ClearDataOffset(VS_Channel ch)
_dll.VS_ClearDataOffset.argtypes = [ctypes.c_void_p]
_dll.VS_ClearDataOffset.restype = ctypes.c_int32

# void* VS_GetSharedMemoryBase(VS_Channel ch)
_dll.VS_GetSharedMemoryBase.argtypes = [ctypes.c_void_p]
_dll.VS_GetSharedMemoryBase.restype = ctypes.c_void_p

# int32_t VS_BeginChunkedTransfer(VS_Channel ch, uint64_t total_size, uint64_t chunk_size)
_dll.VS_BeginChunkedTransfer.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint64]
_dll.VS_BeginChunkedTransfer.restype = ctypes.c_int32

# int32_t VS_SendChunk(VS_Channel ch, uint32_t chunk_index, const uint8_t* data, uint64_t length, uint32_t timeout_ms)
_dll.VS_SendChunk.argtypes = [
    ctypes.c_void_p,
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_ubyte),
    ctypes.c_uint64,
    ctypes.c_uint32
]
_dll.VS_SendChunk.restype = ctypes.c_int32

# int32_t VS_WaitForChunk(VS_Channel ch, uint32_t* out_chunk_index, uint64_t* out_offset, uint64_t* out_length, uint32_t timeout_ms)
_dll.VS_WaitForChunk.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_uint64),
    ctypes.c_uint32
]
_dll.VS_WaitForChunk.restype = ctypes.c_int32

# int32_t VS_AckChunk(VS_Channel ch)
_dll.VS_AckChunk.argtypes = [ctypes.c_void_p]
_dll.VS_AckChunk.restype = ctypes.c_int32

# int32_t VS_WritePy2Ps(VS_Channel ch, const uint8_t* data, uint64_t len, uint32_t timeout_ms, uint64_t* next_seq)
_dll.VS_WritePy2Ps.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_ubyte),
    ctypes.c_uint64,
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint64)
]
_dll.VS_WritePy2Ps.restype = ctypes.c_int32

# int32_t VS_ReadPs2Py(VS_Channel ch, uint8_t* dst, uint64_t dst_cap, uint64_t* out_len, uint32_t timeout_ms)
_dll.VS_ReadPs2Py.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_ubyte),
    ctypes.c_uint64,
    ctypes.POINTER(ctypes.c_uint64),
    ctypes.c_uint32
]
_dll.VS_ReadPs2Py.restype = ctypes.c_int32


class ZeroCopyPointerError(Exception):
    """Error during zero-copy pointer operations."""
    pass


class NativeChannel:
    """Low-level wrapper for VS_Channel handle.
    
    Provides direct access to native shared memory channel for zero-copy operations.
    """
    
    def __init__(self, name: str, frame_bytes: int, num_slots: int = 1):
        """Open native shared memory channel.
        
        Parameters
        ----------
        name : str
            Channel name (shared with PowerShell)
        frame_bytes : int
            Size of each frame buffer
        num_slots : int
            Number of slots (must be 1)
        """
        if num_slots != 1:
            raise ValueError("Only num_slots=1 is supported")
        
        self._handle = _dll.VS_OpenChannel(name, frame_bytes, num_slots, 1)
        if not self._handle:
            raise ZeroCopyPointerError(f"Failed to open channel '{name}'")
        
        self._name = name
        self._frame_bytes = frame_bytes
    
    @property
    def handle(self) -> int:
        """Get raw channel handle for ctypes calls."""
        if self._handle is None:
            raise ZeroCopyPointerError("Channel is closed")
        return self._handle
    
    @property
    def name(self) -> str:
        """Channel name."""
        return self._name
    
    @property
    def frame_bytes(self) -> int:
        """Frame buffer size."""
        return self._frame_bytes
    
    def get_shared_memory_base(self) -> int:
        """Get base address of shared memory region.
        
        Returns
        -------
        int
            Base address pointer as integer (use with ctypes to access memory)
        """
        base_ptr = _dll.VS_GetSharedMemoryBase(self.handle)
        if not base_ptr:
            raise ZeroCopyPointerError("Failed to get shared memory base address")
        return base_ptr
    
    def begin_chunked_transfer(self, total_size: int, chunk_size: int) -> None:
        """Begin a chunked transfer (PowerShell side).
        
        Parameters
        ----------
        total_size : int
            Total size of data to transfer
        chunk_size : int
            Size of each chunk
        """
        result = _dll.VS_BeginChunkedTransfer(self.handle, total_size, chunk_size)
        if result != VS_OK:
            raise ZeroCopyPointerError(f"VS_BeginChunkedTransfer failed with code {result}")
    
    def send_chunk(self, chunk_index: int, data: bytes, timeout_ms: int = 5000) -> None:
        """Send a chunk (PowerShell side).
        
        Parameters
        ----------
        chunk_index : int
            Index of this chunk (0-based)
        data : bytes
            Chunk data
        timeout_ms : int
            Timeout waiting for Python ACK
        """
        data_array = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
        result = _dll.VS_SendChunk(
            self.handle,
            chunk_index,
            data_array,
            len(data),
            timeout_ms
        )
        if result != VS_OK:
            raise ZeroCopyPointerError(f"VS_SendChunk failed with code {result}")
    
    def wait_for_chunk(self, timeout_ms: int = 5000) -> Tuple[int, int, int]:
        """Wait for next chunk (Python side).
        
        Parameters
        ----------
        timeout_ms : int
            Timeout in milliseconds
        
        Returns
        -------
        Tuple[int, int, int]
            (chunk_index, offset, length)
        """
        chunk_index = ctypes.c_uint32(0)
        offset = ctypes.c_uint64(0)
        length = ctypes.c_uint64(0)
        
        result = _dll.VS_WaitForChunk(
            self.handle,
            ctypes.byref(chunk_index),
            ctypes.byref(offset),
            ctypes.byref(length),
            timeout_ms
        )
        
        if result == VS_TIMEOUT:
            raise TimeoutError("Timeout waiting for chunk")
        elif result != VS_OK:
            raise ZeroCopyPointerError(f"VS_WaitForChunk failed with code {result}")
        
        return (chunk_index.value, offset.value, length.value)
    
    def ack_chunk(self) -> None:
        """Acknowledge receipt of chunk (Python side)."""
        result = _dll.VS_AckChunk(self.handle)
        if result != VS_OK:
            raise ZeroCopyPointerError(f"VS_AckChunk failed with code {result}")
    
    def close(self) -> None:
        """Close channel and release resources."""
        if self._handle:
            _dll.VS_CloseChannel(self._handle)
            self._handle = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __del__(self):
        self.close()


def get_pinned_pointer(channel: NativeChannel) -> Optional[Tuple[int, int, int]]:
    """Get pinned pointer from PowerShell.
    
    Parameters
    ----------
    channel : NativeChannel
        Native channel instance
    
    Returns
    -------
    Optional[Tuple[int, int, int]]
        (pointer_address, byte_length, sequence) if valid pointer exists,
        None if no pointer is set
    
    Raises
    ------
    ZeroCopyPointerError
        If API call fails
    """
    ptr = ctypes.c_uint64(0)
    length = ctypes.c_uint64(0)
    seq = ctypes.c_uint64(0)
    
    result = _dll.VS_GetPinnedPointer(
        ctypes.c_void_p(channel.handle),
        ctypes.byref(ptr),
        ctypes.byref(length),
        ctypes.byref(seq)
    )
    
    if result == VS_OK:
        return (ptr.value, length.value, seq.value)
    elif result == VS_ERR_NO_DATA or result == VS_ERR_BAD_STATE:  # No pointer set yet
        return None
    else:
        raise ZeroCopyPointerError(f"VS_GetPinnedPointer failed with code {result}")


def ack_pinned_pointer(channel: NativeChannel) -> None:
    """Acknowledge completion of pointer read.
    
    MUST be called after reading from pointer to signal PowerShell
    that it's safe to unpin the array.
    
    Parameters
    ----------
    channel : NativeChannel
        Native channel instance
    
    Raises
    ------
    ZeroCopyPointerError
        If API call fails
    """
    result = _dll.VS_AckPinnedPointer(ctypes.c_void_p(channel.handle))
    if result != VS_OK:
        raise ZeroCopyPointerError(f"VS_AckPinnedPointer failed with code {result}")


def clear_pinned_pointer(channel: NativeChannel) -> None:
    """Clear pinned pointer metadata.
    
    Parameters
    ----------
    channel : NativeChannel
        Native channel instance
    
    Raises
    ------
    ZeroCopyPointerError
        If API call fails
    """
    result = _dll.VS_ClearPinnedPointer(ctypes.c_void_p(channel.handle))
    if result != VS_OK:
        raise ZeroCopyPointerError(f"VS_ClearPinnedPointer failed with code {result}")


def read_from_pinned_pointer(pointer_address: int, byte_length: int) -> bytes:
    """Read data from pinned pointer.
    
    Creates a ctypes array from the pointer address and copies data to Python bytes.
    
    SAFETY: This assumes the pointer is valid and the memory is accessible.
    The caller MUST ensure the PowerShell side has pinned the array and won't
    unpin until after ack_pinned_pointer() is called.
    
    Parameters
    ----------
    pointer_address : int
        Memory address of pinned array
    byte_length : int
        Number of bytes to read
    
    Returns
    -------
    bytes
        Copy of data from pointer
    
    Raises
    ------
    ZeroCopyPointerError
        If pointer is invalid or memory access fails
    """
    if pointer_address == 0:
        raise ZeroCopyPointerError("Null pointer address")
    if byte_length == 0:
        return b""
    
    try:
        # Create ctypes array from address
        buffer = (ctypes.c_ubyte * byte_length).from_address(pointer_address)
        # Copy to Python bytes
        return bytes(buffer)
    except Exception as e:
        raise ZeroCopyPointerError(f"Failed to read from pointer: {e}") from e


def read_from_pinned_pointer_view(pointer_address: int, byte_length: int) -> memoryview:
    """Get memoryview of pinned pointer (zero-copy on Python side).
    
    Returns a memoryview directly over the PowerShell memory.
    
    CRITICAL: The memoryview MUST be released and ack_pinned_pointer() called
    before PowerShell unpins the array, or you'll get memory corruption.
    
    Parameters
    ----------
    pointer_address : int
        Memory address of pinned array
    byte_length : int
        Number of bytes
    
    Returns
    -------
    memoryview
        Direct view into PowerShell memory (NO COPY)
    
    Raises
    ------
    ZeroCopyPointerError
        If pointer is invalid
    """
    if pointer_address == 0:
        raise ZeroCopyPointerError("Null pointer address")
    if byte_length == 0:
        # Return empty memoryview
        return memoryview(b"")
    
    try:
        # Create ctypes array from address
        buffer = (ctypes.c_ubyte * byte_length).from_address(pointer_address)
        # Return memoryview (zero-copy!)
        return memoryview(buffer)
    except Exception as e:
        raise ZeroCopyPointerError(f"Failed to create memoryview from pointer: {e}") from e
