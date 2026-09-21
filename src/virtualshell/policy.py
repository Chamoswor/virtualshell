"""Guardrails for shells driven by agents (or cautious humans).

An :class:`ExecutionPolicy` attached to a :class:`~virtualshell.Shell` decides,
*before anything executes*, whether a command may run:

- ``deny`` / ``allow`` — glob patterns over the command names a piece of
  PowerShell actually invokes (aliases are resolved first, so ``rm`` is seen
  as ``Remove-Item``).
- ``read_only=True`` — only commands from a curated read-only set (plus any
  extra ``allow`` patterns) may run. A "read-only lane" for exploration.
- ``confirm`` + ``on_confirm`` — patterns that require a human/harness
  callback to approve the command before it runs.
- ``dry_run_destructive=True`` — commands matching ``destructive`` patterns
  are executed inside ``& { $WhatIfPreference = $true; ... }`` so supporting
  cmdlets report what *would* happen instead of doing it.

Command names are extracted with PowerShell's own parser
(``[Language.Parser]::ParseInput``) in the hosted session — nothing is
executed during extraction — and the walk is recursive, so commands inside
``ForEach-Object { ... }`` bodies and other script blocks are seen too.

Honest limits (a policy is a guardrail, not a sandbox): expressions such as
``$x = 5`` contain no commands and always pass; .NET method calls
(``[IO.File]::Delete(...)``) are not command invocations and are not matched;
dynamically-built invocations (``& $cmd``, ``Invoke-Expression $s``) cannot be
resolved statically and are therefore *blocked* whenever the policy is
restrictive (``read_only`` or an ``allow`` list), or when
``block_dynamic=True`` is set explicitly.

Example
-------
>>> from virtualshell import Shell, ExecutionPolicy
>>> def ask(req):
...     return input(f"Allow {req.matched}? [y/N] ") == "y"
>>> sh = Shell(policy=ExecutionPolicy(
...     deny=["Stop-Computer", "Restart-Computer"],
...     confirm=["Remove-*"],
...     on_confirm=ask,
... ))
"""
from __future__ import annotations

import fnmatch
import secrets
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence, Tuple

from ._util import quote_pwsh_literal
from .errors import VirtualShellError

#: Marker used for command invocations whose name cannot be determined
#: statically (``& $cmd``, ``. $something``).
DYNAMIC = "<dynamic>"

#: Command names that evaluate strings as code at runtime; the parser cannot
#: see inside them, so restrictive policies treat them like ``DYNAMIC``.
DYNAMIC_COMMANDS = ("invoke-expression",)

#: Default patterns treated as destructive by ``dry_run_destructive``.
DESTRUCTIVE_DEFAULT: Tuple[str, ...] = (
    "Remove-*", "Set-*", "Stop-*", "Clear-*", "Disable-*", "Uninstall-*",
    "Restart-*", "Reset-*", "Revoke-*", "Suspend-*", "Unregister-*",
    "Dismount-*", "Deny-*", "Block-*", "Format-Volume", "Initialize-Disk",
)

#: Commands a ``read_only=True`` policy allows. Everything here observes
#: without changing system state (session variables can still be assigned:
#: ``$x = ...`` is an expression, not a command).
READ_ONLY_SAFE: Tuple[str, ...] = (
    "Get-*", "Find-*", "Search-*", "Measure-*", "Test-*", "Resolve-*",
    "Select-*", "Sort-*", "Group-*", "Compare-*", "Format-*",
    "ConvertTo-*", "ConvertFrom-*", "Where-Object", "ForEach-Object",
    "Out-String", "Out-Null", "Out-Host", "Out-Default",
    "Write-Output", "Write-Host", "Write-Verbose", "Write-Information",
    "Write-Warning", "Write-Error", "Write-Debug",
    "Join-Path", "Split-Path", "Join-String", "Start-Sleep",
)


@dataclass(frozen=True)
class ConfirmRequest:
    """Passed to ``on_confirm`` when a command matches a ``confirm`` pattern."""

    command: str
    """The full PowerShell text awaiting approval."""
    names: Tuple[str, ...]
    """Every command name detected in it (aliases resolved)."""
    matched: Tuple[str, ...]
    """The subset of names that matched a ``confirm`` pattern."""


@dataclass(frozen=True)
class PolicyDecision:
    """Outcome of :meth:`ExecutionPolicy.inspect` for one command."""

    allowed: bool
    command: str
    """The text to execute — possibly wrapped in a ``-WhatIf`` scope."""
    reason: str = ""
    matched: Tuple[str, ...] = ()
    transformed: bool = False
    """True when ``command`` was rewritten into a WhatIf dry run."""


def _match_any(name: str, patterns: Sequence[str]) -> bool:
    lowered = name.lower()
    return any(fnmatch.fnmatchcase(lowered, p.lower()) for p in patterns)


def _matches(names: Sequence[str], patterns: Sequence[str]) -> Tuple[str, ...]:
    return tuple(n for n in names if _match_any(n, patterns))


@dataclass
class ExecutionPolicy:
    """Rules applied to every command before it reaches PowerShell.

    All patterns are shell-style globs (``Remove-*``), matched
    case-insensitively against resolved command names.

    Evaluation order: ``deny`` -> dynamic invocations -> ``read_only`` /
    ``allow`` -> ``confirm`` -> ``dry_run_destructive``. A command approved
    through ``on_confirm`` runs for real (it is not additionally WhatIf-ed).
    """

    allow: Optional[Sequence[str]] = None
    """When set, every command name must match one of these patterns
    (in ``read_only`` mode: these *extend* the read-only set)."""

    deny: Sequence[str] = ()
    """Command names that are always blocked."""

    confirm: Sequence[str] = ()
    """Command names that require ``on_confirm`` approval before running."""

    on_confirm: Optional[Callable[[ConfirmRequest], bool]] = None
    """Callback approving/rejecting ``confirm`` matches. No callback means
    every ``confirm`` match is blocked."""

    read_only: bool = False
    """Restrict to :data:`READ_ONLY_SAFE` (plus ``allow``) commands."""

    dry_run_destructive: bool = False
    """Run ``destructive`` matches inside ``$WhatIfPreference = $true``."""

    destructive: Sequence[str] = field(default_factory=lambda: DESTRUCTIVE_DEFAULT)
    """Patterns considered destructive for ``dry_run_destructive``."""

    block_dynamic: Optional[bool] = None
    """Block statically unresolvable invocations (``& $cmd``,
    ``Invoke-Expression``). Default (None): blocked exactly when the policy
    is restrictive, i.e. ``read_only`` is set or an ``allow`` list exists."""

    def _blocks_dynamic(self) -> bool:
        if self.block_dynamic is not None:
            return bool(self.block_dynamic)
        return self.read_only or self.allow is not None

    def inspect(self, command: str, names: Sequence[str]) -> PolicyDecision:
        """Decide whether ``command`` (containing ``names``) may run.

        Pure logic — extraction of ``names`` happens separately (see
        :func:`extract_command_names`), so this is unit-testable and reusable.
        """
        names = tuple(names)

        denied = _matches(names, self.deny)
        if denied:
            return PolicyDecision(
                False, command, matched=denied,
                reason=f"blocked by deny pattern: {', '.join(sorted(set(denied)))}")

        dynamic = tuple(
            n for n in names
            if n == DYNAMIC or n.lower() in DYNAMIC_COMMANDS)
        if dynamic and self._blocks_dynamic():
            return PolicyDecision(
                False, command, matched=dynamic,
                reason="dynamic invocation (& $var / Invoke-Expression) cannot "
                       "be inspected statically and this policy is restrictive; "
                       "invoke the command by its literal name instead")

        if self.read_only:
            lane = tuple(READ_ONLY_SAFE) + tuple(self.allow or ())
            outside = tuple(n for n in names if n not in dynamic
                            and not _match_any(n, lane))
            if outside:
                return PolicyDecision(
                    False, command, matched=outside,
                    reason=f"read-only policy blocks: {', '.join(sorted(set(outside)))}")
        elif self.allow is not None:
            outside = tuple(n for n in names if n not in dynamic
                            and not _match_any(n, self.allow))
            if outside:
                return PolicyDecision(
                    False, command, matched=outside,
                    reason=f"not on the allow list: {', '.join(sorted(set(outside)))}")

        needs_confirm = _matches(names, self.confirm)
        if needs_confirm:
            if self.on_confirm is None:
                return PolicyDecision(
                    False, command, matched=needs_confirm,
                    reason=f"requires confirmation ({', '.join(sorted(set(needs_confirm)))}) "
                           "but the policy has no on_confirm handler")
            request = ConfirmRequest(command=command, names=names,
                                     matched=needs_confirm)
            if not self.on_confirm(request):
                return PolicyDecision(
                    False, command, matched=needs_confirm,
                    reason="confirmation was denied by the on_confirm handler")
            # Approved by a human/harness: run for real, skip the dry-run.
            return PolicyDecision(True, command, matched=needs_confirm)

        if self.dry_run_destructive:
            hits = _matches(names, self.destructive)
            if hits:
                wrapped = "& { $WhatIfPreference = $true; " + command + " }"
                return PolicyDecision(True, wrapped, matched=hits,
                                      transformed=True,
                                      reason="destructive command executed as "
                                             "-WhatIf dry run")

        return PolicyDecision(True, command)


# ---------- Command-name extraction ----------

_EXTRACT_TEMPLATE = """\
$__vs_ast = [System.Management.Automation.Language.Parser]::ParseInput({src}, [ref]$null, [ref]$null)
$__vs_found = $__vs_ast.FindAll({{ param($n) $n -is [System.Management.Automation.Language.CommandAst] }}, $true)
$__vs_names = foreach ($__vs_c in $__vs_found) {{
    $__vs_n = $__vs_c.GetCommandName()
    if (-not $__vs_n) {{ '{dynamic}'; continue }}
    $__vs_a = $ExecutionContext.SessionState.InvokeCommand.GetCommand($__vs_n, [System.Management.Automation.CommandTypes]::Alias)
    if ($__vs_a -and $__vs_a.Definition) {{ $__vs_a.Definition }} else {{ $__vs_n }}
}}
[Console]::Out.WriteLine('{beg}' + (@($__vs_names) -join [string][char]1) + '{end}')
"""


def build_extraction_script(command: str) -> Tuple[str, str, str]:
    """Compose the PowerShell snippet that lists the command names in
    ``command`` without executing any of it.

    Returns ``(script, begin_marker, end_marker)``; the names appear between
    the markers on stdout, separated by ``\\x01``.
    """
    token = secrets.token_hex(4)
    beg = f"<<VSPN_{token}>>"
    end = f"<<VSPN_END_{token}>>"
    script = _EXTRACT_TEMPLATE.format(
        src=quote_pwsh_literal(command), dynamic=DYNAMIC, beg=beg, end=end)
    return script, beg, end


def parse_extraction_output(out: str, beg: str, end: str) -> Tuple[str, ...]:
    """Parse the extraction snippet's stdout into a tuple of command names.

    Raises ``VirtualShellError`` when the markers are missing (fail closed:
    a policy must never wave a command through unseen).
    """
    start = out.find(beg)
    stop = out.find(end)
    if start == -1 or stop == -1 or stop < start:
        raise VirtualShellError(
            "policy inspection failed: could not parse the command "
            "(the hosted PowerShell did not return the parser's result)")
    payload = out[start + len(beg):stop]
    if not payload:
        return ()
    return tuple(n for n in payload.split("\x01") if n)
