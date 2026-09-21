"""Output budgeting: bounded views of large command output, with paging.

Large PowerShell output (an unfiltered ``Get-Process``, a big log file) is
expensive for LLM agents — a single result can consume tens of thousands of
tokens of context. ``Shell.run(cmd, max_output=...)`` keeps the *full* output
in an in-memory :class:`OutputStore` and returns only a head+tail view with an
inline marker naming the continuation key; :meth:`Shell.fetch_output` pages
through the rest on demand.

The marker is a plain line of text, readable by humans and models alike::

    ... [virtualshell: output truncated - 51203 chars / 1200 lines total,
         showing first 2666 and last 1290 chars;
         more: fetch_output('a1b2c3d4', offset=2666)] ...
"""
from __future__ import annotations

import secrets
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class OutputSlice:
    """One page of a stored command output (see ``Shell.fetch_output``)."""

    key: str
    """Continuation key the page was fetched from."""
    text: str
    """The page itself."""
    offset: int
    """Character offset of ``text`` within the full output."""
    next_offset: Optional[int]
    """Offset to pass to ``fetch_output`` for the next page; None at the end."""
    total_chars: int
    """Length of the full output in characters."""
    total_lines: int
    """Number of lines in the full output."""

    @property
    def at_end(self) -> bool:
        """True when this page reaches the end of the stored output."""
        return self.next_offset is None


class OutputStore:
    """Bounded in-memory store of full command outputs, keyed for paging.

    Entries are evicted oldest-first once ``max_entries`` or
    ``max_total_chars`` is exceeded, so a long-lived agent session cannot
    accumulate unbounded memory. Lookup of an evicted key raises ``KeyError``
    with a message that says so.
    """

    def __init__(self, max_entries: int = 32, max_total_chars: int = 64_000_000) -> None:
        self._entries: "OrderedDict[str, str]" = OrderedDict()
        self._max_entries = int(max_entries)
        self._max_total_chars = int(max_total_chars)
        self._total_chars = 0

    def put(self, text: str) -> str:
        """Store ``text`` and return its continuation key."""
        key = secrets.token_hex(4)
        self._entries[key] = text
        self._total_chars += len(text)
        self._evict()
        return key

    def get(self, key: str) -> str:
        try:
            return self._entries[key]
        except KeyError:
            raise KeyError(
                f"No stored output for key {key!r}: it was evicted or never "
                "existed (the store keeps the most recent "
                f"{self._max_entries} truncated outputs)") from None

    def slice(self, key: str, offset: int = 0, size: int = 4000) -> OutputSlice:
        """Return one page of the stored output starting at ``offset``."""
        text = self.get(key)
        total = len(text)
        offset = max(0, int(offset))
        size = int(size)
        if size <= 0:
            size = total
        end = min(offset + size, total)
        return OutputSlice(
            key=key,
            text=text[offset:end],
            offset=offset,
            next_offset=end if end < total else None,
            total_chars=total,
            total_lines=text.count("\n") + 1 if text else 0,
        )

    def clear(self) -> None:
        self._entries.clear()
        self._total_chars = 0

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: object) -> bool:
        return key in self._entries

    def _evict(self) -> None:
        while self._entries and (
            len(self._entries) > self._max_entries
            or self._total_chars > self._max_total_chars
        ):
            _, dropped = self._entries.popitem(last=False)
            self._total_chars -= len(dropped)


def _snap_to_line(text: str, pos: int, *, backwards: bool) -> int:
    """Move ``pos`` to the nearest newline boundary (within 200 chars) so the
    cut does not land mid-line; falls back to ``pos`` when no newline is near."""
    window = 200
    if backwards:
        nl = text.rfind("\n", max(0, pos - window), pos)
        return nl + 1 if nl != -1 else pos
    nl = text.find("\n", pos, pos + window)
    return nl if nl != -1 else pos


def apply_budget(
    text: str,
    max_output: int,
    store: OutputStore,
    *,
    label: str = "output",
) -> Tuple[str, Optional[str]]:
    """Return ``text`` unchanged when it fits ``max_output`` characters,
    otherwise a head+tail view with an inline continuation marker.

    Returns ``(visible_text, key)`` where ``key`` is the continuation key of
    the stored full text, or None when nothing was truncated. The visible text
    is guaranteed to be at most ``max_output`` characters (provided
    ``max_output`` is large enough to hold the marker itself).
    """
    if max_output <= 0 or len(text) <= max_output:
        return text, None

    key = store.put(text)
    total_lines = text.count("\n") + 1

    # The marker length depends on the numbers in it, which depend on the cut
    # positions; using the full text's dimensions for the estimate is stable.
    marker_template = (
        "\n... [virtualshell: {label} truncated - {total} chars / {lines} lines total, "
        "showing first {head} and last {tail} chars; "
        "more: fetch_output('{key}', offset={head})] ...\n"
    )
    marker_probe = marker_template.format(
        label=label, total=len(text), lines=total_lines,
        head=len(text), tail=len(text), key=key)

    room = max_output - len(marker_probe)
    if room < 80:
        # Budget too small for head+tail; hard head cut with a short marker.
        short = f"... [truncated; full {len(text)} chars: fetch_output('{key}')]"
        head = max(0, max_output - len(short) - 1)
        return text[:head] + "\n" + short if head else short, key

    head_len = _snap_to_line(text, (room * 2) // 3, backwards=True)
    tail_start = _snap_to_line(text, len(text) - (room - head_len), backwards=False)
    tail_len = len(text) - tail_start

    marker = marker_template.format(
        label=label, total=len(text), lines=total_lines,
        head=head_len, tail=tail_len, key=key)
    return text[:head_len] + marker + text[tail_start:], key
