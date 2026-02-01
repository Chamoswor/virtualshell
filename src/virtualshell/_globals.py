from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path
if TYPE_CHECKING:
    pass

_MODULE_DIR = Path(__file__).parent.resolve()

_VS_CORE_CPP_MODULE = f"{__package__}._core"
_VS_SHM_CPP_MODULE_PATH = _MODULE_DIR / "_vs_shm.dll"

