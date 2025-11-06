from __future__ import annotations
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shell import ExecutionResult, BatchProgress, Shell, ExitCode, SharedMemoryChannel, create_shared_memory_channel
    from .shared_memory_bridge import SharedMemoryBridge, PublishResult, ZeroCopyView

try:
    from ._version import version as __version__
except Exception:
    __version__ = "1.1.2"


from .errors import (
    VirtualShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

__all__ = [
    "VirtualShellError", "PowerShellNotFoundError",
    "ExecutionTimeoutError", "ExecutionError",
    "__version__", "Shell", "ExecutionResult", "BatchProgress", "ExitCode",
    "SharedMemoryChannel", "create_shared_memory_channel", 
    "SharedMemoryBridge", "PublishResult", "ZeroCopyView",
]

def __getattr__(name: str):
    if name in {"Shell", "ExecutionResult", "BatchProgress", "ExitCode", "SharedMemoryChannel", "create_shared_memory_channel"}:
        mod = import_module(".shell", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    if name in {"SharedMemoryBridge", "PublishResult", "ZeroCopyView"}:
        mod = import_module(".shared_memory_bridge", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

def __dir__():
    return sorted(__all__)
