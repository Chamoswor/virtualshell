"""Zero-Copy Bridge - Integrated with Shell class

Design:
- Python owns channel lifecycle AND PowerShell session
- Shell class is used to control PowerShell side
- All transfers are chunked (configurable chunk size)
- Zero-copy via memoryview of shared memory
"""
import ctypes
import os
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .shell import Shell

__all__ = ["ZeroCopyBridge", "find_dll"]

_IS_WINDOWS = os.name == "nt"

# =============================================================================
# DLL LOADING
# =============================================================================

def find_dll() -> Path:
    """Find win_pwsh.dll."""
    if not _IS_WINDOWS:
        raise RuntimeError("win_pwsh.dll is only available on Windows platforms")

    if env_path := os.environ.get("VIRTUALSHELL_WIN_PWSH_DLL"):
        dll_path = Path(env_path)
        if dll_path.is_file():
            return dll_path
        elif dll_path.is_dir():
            dll_path = dll_path / "win_pwsh.dll"
            if dll_path.exists():
                return dll_path
    
    script_dir = Path(__file__).parent
    candidates = [
        script_dir / "win_pwsh.dll",
        script_dir.parent.parent / "build" / "win_pwsh_dll" / "Release" / "win_pwsh.dll",
        script_dir.parent.parent / "build" / "win_pwsh_dll" / "Debug" / "win_pwsh.dll",
        script_dir.parent.parent / "win_pwsh_dll" / "bin" / "win_pwsh.dll",
    ]
    
    for candidate in candidates:
        if candidate.exists():
            return candidate
    
    raise FileNotFoundError("win_pwsh.dll not found. Set VIRTUALSHELL_WIN_PWSH_DLL environment variable.")


def _load_win_dll() -> tuple[Optional[Path], Optional[ctypes.CDLL]]:
    """Attempt to load the Windows DLL, returning path and handle when available."""
    if not _IS_WINDOWS:
        return None, None

    dll_path = find_dll()
    try:
        return dll_path, ctypes.CDLL(str(dll_path))
    except OSError as exc:
        raise RuntimeError(f"Failed to load win_pwsh.dll from {dll_path}: {exc}") from exc

# Load DLL (Windows only)
_dll_path, _dll = _load_win_dll()

# Status codes
VS_OK = 0
VS_TIMEOUT = 1
VS_WOULD_BLOCK = 2
VS_ERR_INVALID = -1
VS_ERR_SYSTEM = -2
VS_ERR_BAD_STATE = -3
VS_ERR_TOO_LARGE = -4

# Function signatures
if _dll is not None:
    _dll.VS_CreateChannel.argtypes = [ctypes.c_wchar_p, ctypes.c_uint64]
    _dll.VS_CreateChannel.restype = ctypes.c_void_p

    _dll.VS_DestroyChannel.argtypes = [ctypes.c_void_p]
    _dll.VS_DestroyChannel.restype = None

    _dll.VS_BeginPy2PsTransfer.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint64]
    _dll.VS_BeginPy2PsTransfer.restype = ctypes.c_int32

    _dll.VS_SendPy2PsChunk.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_ubyte),
        ctypes.c_uint64, ctypes.c_uint32
    ]
    _dll.VS_SendPy2PsChunk.restype = ctypes.c_int32

    _dll.VS_WaitPy2PsAck.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    _dll.VS_WaitPy2PsAck.restype = ctypes.c_int32

    _dll.VS_FinishPy2PsTransfer.argtypes = [ctypes.c_void_p]
    _dll.VS_FinishPy2PsTransfer.restype = ctypes.c_int32

    _dll.VS_WaitPs2PyChunk.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.c_uint32
    ]
    _dll.VS_WaitPs2PyChunk.restype = ctypes.c_int32

    _dll.VS_AckPs2PyChunk.argtypes = [ctypes.c_void_p]
    _dll.VS_AckPs2PyChunk.restype = ctypes.c_int32

    _dll.VS_IsPs2PyComplete.argtypes = [ctypes.c_void_p]
    _dll.VS_IsPs2PyComplete.restype = ctypes.c_int32

    _dll.VS_GetMemoryBase.argtypes = [ctypes.c_void_p]
    _dll.VS_GetMemoryBase.restype = ctypes.c_void_p

# =============================================================================
# INTEGRATED BRIDGE WITH SHELL
# =============================================================================

class ZeroCopyBridge:
    """Zero-copy bridge that controls both Python and PowerShell sides.
    
    Uses Shell class to manage PowerShell session.
    
    Example:
        from virtualshell import Shell
        shell = Shell()
        bridge = ZeroCopyBridge(shell, channel_name="my_channel")
        
        # Send from PowerShell to Python
        bridge.send_from_powershell("$myvar")  # PowerShell sends in background
        data = bridge.receive()  # Python receives
        
        # Send from Python to PowerShell
        bridge.send(b"hello")  # Python sends in background
        bridge.receive_to_powershell("$result")  # PowerShell receives
    """
    
    def __init__(
        self,
        shell: "Shell",
        channel_name: str = "VS_Bridge",
        frame_mb: int = 64,
        chunk_mb: int = 4,
        scope: str = "Local"
    ):
        """Initialize bridge with Shell instance.
        
        Args:
            shell: Shell instance to use for PowerShell side
            channel_name: Channel name (without prefix)
            frame_mb: Frame size in MB (per direction)
            chunk_mb: Default chunk size in MB
            scope: "Local" or "Global"
        """
        dll = _dll
        if dll is None:
            raise RuntimeError("ZeroCopyBridge requires win_pwsh.dll and is only available on Windows")

        self.shell = shell
        self.channel_name = f"{scope}\\{channel_name}"
        self.channel_name_short = channel_name  # Without prefix
        self._scope = scope
        self.frame_bytes = frame_mb * 1024 * 1024
        self.default_chunk_bytes = chunk_mb * 1024 * 1024
        self._active_jobs = []  # Track active PowerShell jobs
        self._active_futures: List[Future[Any]] = []
        self._dll = dll
        
        # Create channel (Python owns it)
        self._handle = dll.VS_CreateChannel(self.channel_name, self.frame_bytes)
        if not self._handle:
            raise RuntimeError(f"Failed to create channel: {self.channel_name}")
        
        # Get shared memory base
        self._mem_base = dll.VS_GetMemoryBase(self._handle)
        if not self._mem_base:
            dll.VS_DestroyChannel(self._handle)
            raise RuntimeError("Failed to get shared memory base")
        
        self._mem_base_addr = self._mem_base if isinstance(self._mem_base, int) else self._mem_base.value
        
        # Load PowerShell bridge module
        bridge_script = Path(__file__).parent / "zero_copy_bridge.ps1"
        dll_path = _dll_path
        if dll_path is None:
            raise RuntimeError("win_pwsh.dll path is unavailable")
        
        # Store paths for job execution
        self._ps_script_path = str(bridge_script.absolute())
        self._ps_dll_path = str(dll_path.absolute())
        
        # Load PowerShell bridge script (set execution policy bypass for this session)
        self.shell.run("Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force")
        self.shell.run(f". '{bridge_script}'")
        init_cmd = f"Initialize-VSNative -PreferredPath '{self._ps_dll_path}'"
        self.shell.run(init_cmd)
    
    def __del__(self):
        # Clean up any active jobs
        if hasattr(self, '_active_jobs'):
            for job_id in self._active_jobs:
                try:
                    self.shell.run(f"Stop-Job -Id {job_id} -ErrorAction SilentlyContinue; Remove-Job -Id {job_id} -Force -ErrorAction SilentlyContinue")
                except:
                    pass
        
        if hasattr(self, '_active_futures'):
            for future in list(self._active_futures):
                future.cancel()
            self._active_futures.clear()

        if hasattr(self, '_handle') and self._handle and hasattr(self, "_dll"):
            try:
                self._dll.VS_DestroyChannel(self._handle)
            except Exception:
                pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        # Clean up jobs
        for job_id in self._active_jobs:
            try:
                self.shell.run(f"Stop-Job -Id {job_id} -ErrorAction SilentlyContinue; Remove-Job -Id {job_id} -Force -ErrorAction SilentlyContinue")
            except:
                pass
        self._active_jobs.clear()

        for future in self._active_futures:
            future.cancel()
        self._active_futures.clear()
        
        if self._handle:
            self._dll.VS_DestroyChannel(self._handle)
            self._handle = None

    def _track_future(self, future: Future[Any]) -> Future[Any]:
        self._active_futures.append(future)

        def _remove(_fut: Future[Any]) -> None:
            try:
                self._active_futures.remove(_fut)
            except ValueError:
                pass

        future.add_done_callback(_remove)
        return future
    
    # =========================================================================
    # POWERSHELL → PYTHON
    # =========================================================================
    
    def send_from_powershell(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
    ):
        """Trigger PowerShell to send variable to Python.
        
        Uses one-shot PowerShell Job that loads module and sends.
        
        Args:
            variable: PowerShell variable to send (e.g., "$mydata")
            timeout: Timeout in seconds
            use_job: If True, use PowerShell Job (recommended)
        
        Returns:
              Job ID (int) if use_job=True, else concurrent.futures.Future
        """
        # Use run_async to avoid per-transfer ThreadJob overhead
        cmd = f"""
            Send-VariableToPython -ChannelName '{self.channel_name_short}' -Variable {variable} -ChunkSizeMB {self.default_chunk_bytes // (1024*1024)} -TimeoutSeconds {int(timeout)} -Scope '{self._scope}'
        """
        command = cmd.strip()
        future = self.shell.run_async(command, timeout=timeout)
        return self._track_future(future)
    
    def receive(
        self,
        *,
        timeout: float = 30.0,
        return_memoryview: bool = False
    ) -> bytes | memoryview:
        """Receive data from PowerShell (zero-copy).
        
        Args:
            timeout: Timeout in seconds
            return_memoryview: If True, return memoryview (zero-copy)
        
        Returns:
            bytes or memoryview of received data
        """
        dll = self._dll
        timeout_ms = int(timeout * 1000)
        chunks = []
        
        while True:
            chunk_index = ctypes.c_uint32()
            chunk_offset = ctypes.c_uint64()
            chunk_length = ctypes.c_uint64()
            
            result = dll.VS_WaitPs2PyChunk(
                self._handle,
                ctypes.byref(chunk_index),
                ctypes.byref(chunk_offset),
                ctypes.byref(chunk_length),
                timeout_ms
            )
            
            if result == VS_TIMEOUT:
                # PowerShell might have finished immediately after the last ACK
                if dll.VS_IsPs2PyComplete(self._handle):
                    break
                raise TimeoutError("Timeout waiting for PowerShell chunk")
            elif result != VS_OK:
                raise RuntimeError(f"VS_WaitPs2PyChunk failed: {result}")
            
            # Zero-copy read from shared memory
            ptr = ctypes.c_void_p(self._mem_base_addr + chunk_offset.value)
            c_array = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_ubyte * chunk_length.value)).contents
            chunk_mv = memoryview(c_array)
            
            chunks.append(bytes(chunk_mv))
            
            # Acknowledge chunk
            result = dll.VS_AckPs2PyChunk(self._handle)
            if result != VS_OK:
                raise RuntimeError(f"VS_AckPs2PyChunk failed: {result}")
            
            # Check if complete
            is_complete = dll.VS_IsPs2PyComplete(self._handle)
            if not is_complete:
                poll_deadline = time.perf_counter() + min(0.2, timeout)
                while time.perf_counter() < poll_deadline:
                    if dll.VS_IsPs2PyComplete(self._handle):
                        is_complete = 1
                        break
                    time.sleep(0.001)
            if is_complete:
                break
        
        if not chunks:
            data = b""
        else:
            data = chunks[0] if len(chunks) == 1 else b"".join(chunks)
        
        return memoryview(data) if return_memoryview else data
    
    # =========================================================================
    # PYTHON → POWERSHELL
    # =========================================================================
    
    def send(
        self,
        data: bytes,
        *,
        chunk_size: Optional[int] = None,
        timeout: float = 30.0
    ) -> None:
        """Send bytes from Python to PowerShell.
        
        Args:
            data: Bytes to send
            chunk_size: Chunk size in bytes (default: self.default_chunk_bytes)
            timeout: Timeout in seconds
        """
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        
        chunk_bytes = chunk_size or self.default_chunk_bytes
        timeout_ms = int(timeout * 1000)
        
        # Begin transfer
        dll = self._dll
        result = dll.VS_BeginPy2PsTransfer(self._handle, len(data), chunk_bytes)
        if result != VS_OK:
            raise RuntimeError(f"VS_BeginPy2PsTransfer failed: {result}")
        
        # Send chunks
        num_chunks = (len(data) + chunk_bytes - 1) // chunk_bytes
        
        for i in range(num_chunks):
            offset = i * chunk_bytes
            chunk_len = min(chunk_bytes, len(data) - offset)
            chunk_data = data[offset:offset + chunk_len]
            
            c_array = (ctypes.c_ubyte * len(chunk_data)).from_buffer_copy(chunk_data)
            
            result = dll.VS_SendPy2PsChunk(
                self._handle, i, c_array, chunk_len, timeout_ms
            )
            if result != VS_OK:
                raise RuntimeError(f"VS_SendPy2PsChunk failed at chunk {i}: {result}")
            
            result = dll.VS_WaitPy2PsAck(self._handle, timeout_ms)
            if result == VS_TIMEOUT:
                raise TimeoutError(f"Timeout waiting for PowerShell ACK at chunk {i}")
            elif result != VS_OK:
                raise RuntimeError(f"VS_WaitPy2PsAck failed at chunk {i}: {result}")
        
        # Mark complete
        result = dll.VS_FinishPy2PsTransfer(self._handle)
        if result != VS_OK:
            raise RuntimeError(f"VS_FinishPy2PsTransfer failed: {result}")
    
    def receive_to_powershell(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
    ):
        """Trigger PowerShell to receive data into variable.
        
        Args:
            variable: PowerShell variable name (e.g., "$mydata")
            timeout: Timeout in seconds
        
        Returns:
            Future representing the receive operation
        """
        var_name = variable.lstrip('$')
        
        # Use run_async completion future instead of spawning jobs
        cmd = f"""
            Receive-VariableFromPython -ChannelName '{self.channel_name_short}' -VariableName '{var_name}' -TimeoutSeconds {int(timeout)} -Scope '{self._scope}'
        """
        command = cmd.strip()
        future = self.shell.run_async(command, timeout=timeout)
        return self._track_future(future)
    
    @property
    def handle(self) -> Any:
        """Get channel handle."""
        return self._handle
    
    @property
    def memory_base(self) -> Any:
        """Get shared memory base address."""
        return self._mem_base_addr
