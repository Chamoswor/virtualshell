from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

_CORE_MODULE_NAME = f"{__package__}._core"

try:
    # Vi ignorerer typen her ved runtime, siden vi fikser det under
    core = importlib.import_module(_CORE_MODULE_NAME)
except Exception as e:
    raise ImportError(
        f"Failed to import the compiled extension '{_CORE_MODULE_NAME}'. "
        "Make sure it was built and matches this Python/platform."
    ) from e

if TYPE_CHECKING:
    from ._protocols import (
        ExecutionResultLike as ExecutionResult,
        BatchProgressLike as BatchProgress,
        ConfigLike as Config,
        VirtualShellLike as VirtualShell,
    )
else:
    ExecutionResult = core.ExecutionResult
    BatchProgress = core.BatchProgress
    Config = core.Config
    VirtualShell = core.VirtualShell