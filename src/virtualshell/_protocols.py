from __future__ import annotations

from enum import IntEnum
import sys

from typing import (
    Protocol, 
    Any, 
    Callable,
    List,
    Dict,
    Self,
    runtime_checkable,
)
from concurrent.futures import Future

class ExitCode(IntEnum):
    SUCCESS = 0
    GENERAL_ERROR = 1
    TIMEOUT = -1
    RESTARTING = -2  # Internal use; not from PowerShell itself.
    NOT_RUNNING = -3  # Internal use; not from PowerShell itself.

@runtime_checkable
class ExecutionResultLike(Protocol):
    out: Any
    err: Any
    exit_code: ExitCode
    success: bool
    execution_time: float

@runtime_checkable
class BatchProgressLike(Protocol):
    currentCommand: int
    totalCommands: int
    lastResult: ExecutionResultLike
    isComplete: bool
    allResults: List[ExecutionResultLike]

    @property
    def header_bytes(self) -> int: ...

    @property
    def frame_bytes(self) -> int: ...

@runtime_checkable
class ConfigLike(Protocol):
    powershell_path: str = "pwsh"
    working_directory: str = ""
    capture_output: bool = True
    capture_error: bool = True
    auto_restart_on_timeout: bool = True
    timeout_seconds: int = 30
    environment: dict[str, str]
    initial_commands: List[str]
    restore_script_path: str = ""
    session_snapshot_path: str = ""
    stdin_buffer_size: int = 64 * 1024


    def __init__(self) -> None:
        ...

@runtime_checkable
class PsProxyLike(Protocol):
    
    # def_property_readonly oversettes til en @property i Python
    @property
    def type_name(self) -> str:
        ...

    def proxy_schema(self) -> dict[str, Any]:
        ...

    def proxy_multi_call(self, func: Callable[..., Any], *args: Any) -> List[Any]:
            ...

    def __getattr__(self, name: str) -> Any:
        ...

    def __setattr__(self, name: str, value: Any) -> None:
        ...

    def __dir__(self) -> List[str]:
        ...

@runtime_checkable
class VirtualShellLike(Protocol):
    
    # ---------------------------------------------------------
    # Process control
    # ---------------------------------------------------------
    def start(self) -> bool:
        ...

    def stop(self, force: bool = False) -> None:
        ...

    def is_alive(self) -> bool:
        ...

    def is_restarting(self) -> bool:
        ...

    def get_process_id(self) -> int:
        ...

    # ---------------------------------------------------------
    # Sync commands
    # ---------------------------------------------------------
    def execute(self, command: str, timeout_seconds: float = 0.0) -> ExecutionResultLike:
        ...

    def execute_batch(self, commands: List[str], timeout_seconds: float = 0.0) -> List[ExecutionResultLike]:
        ...

    def execute_script(
        self, 
        script_path: str, 
        args: List[str] = ..., 
        timeout_seconds: float = 0.0, 
        dot_source: bool = False, 
        raise_on_error: bool = False
    ) -> Any:
        ...

    def execute_script_kv(
        self, 
        script_path: str, 
        named_args: Dict[str, str], 
        timeout_seconds: float = 0.0, 
        dot_source: bool = False, 
        raise_on_error: bool = False
    ) -> Any:
        ...

    # ---------------------------------------------------------
    # Async commands
    # ---------------------------------------------------------
    def execute_async(
        self, 
        command: str, 
        callback: Callable[..., Any] | None = None, 
        timeout_seconds: float = 0.0
    ) -> Future[Any]:
        ...

    def execute_async_batch(
        self, 
        commands: List[str], 
        progress_callback: Callable[..., Any] | None = None, 
        stop_on_first_error: bool = True, 
        per_command_timeout_seconds: float = 0.0
    ) -> Future[List[Any]]:
        ...

    def execute_async_script(
        self, 
        script_path: str, 
        args: List[str] = ..., 
        callback: Callable[..., Any] | None = None, 
        timeout_seconds: float = 0.0, 
        dot_source: bool = False, 
        raise_on_error: bool = False
    ) -> Future[Any]:
        ...

    def execute_async_script_kv(
        self, 
        script_path: str, 
        named_args: Dict[str, str], 
        timeout_seconds: float = 0.0, 
        dot_source: bool = False, 
        raise_on_error: bool = False
    ) -> Future[Any]:
        ...

    # ---------------------------------------------------------
    # Direct I/O / env / modules
    # ---------------------------------------------------------
    def send_input(self, input: str) -> bool:
        ...

    def read_output(self, blocking: bool = False) -> str:
        ...

    def read_error(self, blocking: bool = False) -> str:
        ...

    def set_working_directory(self, directory: str) -> bool:
        ...

    def get_working_directory(self) -> str:
        ...

    def set_environment_variable(self, name: str, value: str) -> bool:
        ...

    def get_environment_variable(self, name: str) -> str:
        ...

    def is_module_available(self, module_name: str) -> bool:
        ...

    def import_module(self, module_name: str) -> bool:
        ...

    def get_powershell_version(self) -> str:
        ...

    def get_available_modules(self) -> List[str]:
        ...

    # ---------------------------------------------------------
    # Configuration and Internals
    # ---------------------------------------------------------
    def get_config(self) -> ConfigLike:
        ...

    def update_config(self, config: ConfigLike) -> bool:
        ...

    def get_shared_ptr(self) -> Self:
            ...

    def make_proxy(
        self, 
        type_name: str, 
        object_ref: str = "$obj", 
        depth: int = 4
    ) -> PsProxyLike:
        ...

    # ---------------------------------------------------------
    # Context Manager
    # ---------------------------------------------------------
    def __enter__(self) -> "VirtualShellLike":
        ...

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ...