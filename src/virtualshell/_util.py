"""Small shared helpers used across virtualshell modules."""
from __future__ import annotations

from typing import List


def quote_pwsh_literal(s: str) -> str:
    """Return a PowerShell *single-quoted* literal for arbitrary text `s`.

    Rules:
    - Empty string => `''` (empty single-quoted literal)
    - Single quotes are doubled inside the literal per PowerShell rules.
    - No interpolation/expansion occurs within single quotes in PowerShell.

    This is safe for *data-as-argument* scenarios, not for embedding raw code.
    Use it to construct commands like: `Write-Output {literal}`.
    """
    if not s:
        return "''"
    out: List[str] = []
    append = out.append
    append("'")
    for ch in s:
        append("''" if ch == "'" else ch)
    append("'")
    return "".join(out)
