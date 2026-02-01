from __future__ import annotations
from importlib import import_module
from typing import TYPE_CHECKING
import platform

if TYPE_CHECKING:
    from .shell import ExecutionResult, BatchProgress, Shell, ExitCode
    from .zero_copy_bridge_shell import ZeroCopyBridge, PSObject

try:
    from ._version import version as __version__
except Exception:
    __version__ = "0.0.0"

from . import _globals as _g

from .errors import (
    VirtualShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

if platform.system() == 'Windows':
    __all__ = [
        "VirtualShellError", "PowerShellNotFoundError",
        "ExecutionTimeoutError", "ExecutionError",
        "__version__", "Shell", "ExecutionResult", "BatchProgress", "ExitCode",
        "ZeroCopyBridge", "PSObject",
    ]
else:
    __all__ = [
        "VirtualShellError", "PowerShellNotFoundError",
        "ExecutionTimeoutError", "ExecutionError",
        "__version__", "Shell", "ExecutionResult", "BatchProgress", "ExitCode"
    ]

# Lazy loading of submodules and attributes to avoid importing compiled extension at package import time
def __getattr__(name: str):
    if name in {"Shell", "ExecutionResult", "BatchProgress", "ExitCode"}:
        mod = import_module(".shell", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    if  name in {"ZeroCopyBridge", "PSObject"}:
        if platform.system() == 'Windows':
            mod = import_module(".zero_copy_bridge_shell", __name__)
            obj = getattr(mod, name)
            globals()[name] = obj
            return obj
        else:
            raise ImportError(f"{name} is only available on Windows platforms.")
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

def __dir__():
    return sorted(__all__)
