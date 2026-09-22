from __future__ import annotations
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shell import ExecutionResult, BatchProgress, Shell, ExitCode, Config, Checkpoint
    from .policy import ExecutionPolicy, ConfirmRequest, PolicyDecision
    from .output import OutputSlice
    from ._util import quote_pwsh_literal
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
    PromptBlockedError,
    PolicyViolationError,
    ScriptBlockDelegateWarning,
)

__all__ = [
    "VirtualShellError", "PowerShellNotFoundError",
    "ExecutionTimeoutError", "ExecutionError",
    "PromptBlockedError", "PolicyViolationError",
    "ScriptBlockDelegateWarning",
    "__version__", "Shell", "ExecutionResult", "BatchProgress", "ExitCode", "Config",
    "Checkpoint", "ExecutionPolicy", "ConfirmRequest", "PolicyDecision", "OutputSlice",
    "quote_pwsh_literal",
    "ZeroCopyBridge", "PSObject",
]

# Lazy loading of submodules and attributes to avoid importing compiled extension at package import time
def __getattr__(name: str):
    if name in {"Shell", "ExecutionResult", "BatchProgress", "ExitCode", "Config", "Checkpoint"}:
        mod = import_module(".shell", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    # Pure-Python modules: importable without the compiled extension.
    if name in {"ExecutionPolicy", "ConfirmRequest", "PolicyDecision"}:
        mod = import_module(".policy", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    if name == "OutputSlice":
        mod = import_module(".output", __name__)
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    if name == "quote_pwsh_literal":
        mod = import_module("._util", __name__)
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
