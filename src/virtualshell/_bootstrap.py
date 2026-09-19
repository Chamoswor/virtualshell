"""Guarded import of the compiled pybind11 extension.

Importing the extension fails if it was never built, or was built for a
different Python version or platform. Every import of it is funnelled
through here so that failure surfaces as a single, actionable ImportError
instead of a bare ``ModuleNotFoundError``.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

_CORE_MODULE_NAME = f"{__package__}._core"

try:
    core: Any = importlib.import_module(_CORE_MODULE_NAME)
except Exception as e:
    raise ImportError(
        f"Failed to import the compiled extension '{_CORE_MODULE_NAME}'. "
        "Make sure it was built and matches this Python/platform."
    ) from e
