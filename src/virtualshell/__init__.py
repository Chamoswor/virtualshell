from __future__ import annotations
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shell import ExecutionResult, BatchProgress, Shell, ExitCode, Config
    from .zero_copy_bridge_shell import ZeroCopyBridge, PSObject

try:
    from ._version import version as __version__
except Exception:
    __version__ = "0.0.0"

from .errors import (
    VirtualShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

__all__ = [
    "VirtualShellError", "PowerShellNotFoundError",
    "ExecutionTimeoutError", "ExecutionError",
    "__version__", "Shell", "ExecutionResult", "BatchProgress", "ExitCode", "Config",
    "ZeroCopyBridge", "PSObject",
]

# Lazy loading of submodules and attributes to avoid importing compiled extension at package import time
def __getattr__(name: str):
    if name in {"Shell", "ExecutionResult", "BatchProgress", "ExitCode", "Config"}:
        mod = import_module(".shell", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    if name in {"ZeroCopyBridge", "PSObject"}:
        mod = import_module(".zero_copy_bridge_shell", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

def __dir__():
    return sorted(__all__)
