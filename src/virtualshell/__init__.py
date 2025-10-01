from __future__ import annotations
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shell import ExecutionResult, Shell

try:
    from ._version import version as __version__
except Exception:
    __version__ = "0.1.2"


from .errors import (
    VirtualShellError,
    PowerShellNotFoundError,
    ExecutionTimeoutError,
    ExecutionError,
)

__all__ = [
    "VirtualShellError", "PowerShellNotFoundError",
    "ExecutionTimeoutError", "ExecutionError",
    "__version__", "Shell", "ExecutionResult",
]

def __getattr__(name: str):
    if name in {"Shell", "ExecutionResult"}:
        mod = import_module(".shell", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

def __dir__():
    return sorted(__all__)
