"""Typed exceptions and warnings raised by virtualshell.

Every error raised by this library derives from ``VirtualShellError``, so a
single ``except VirtualShellError`` catches them all while the subclasses stay
precise enough for targeted retry / telemetry policies. Warning categories
(``ScriptBlockDelegateWarning``) derive from ``UserWarning`` instead, so they
flow through the standard :mod:`warnings` machinery.
"""


class VirtualShellError(RuntimeError):
    """Base class for every error virtualshell raises."""


class PowerShellNotFoundError(VirtualShellError):
    """No usable PowerShell executable could be started."""


class ExecutionTimeoutError(VirtualShellError):
    """A command exceeded its timeout budget."""


class ExecutionError(VirtualShellError):
    """A command failed and ``raise_on_error`` was requested."""


class PromptBlockedError(ExecutionError):
    """A command tried to prompt for interactive input (``Read-Host``,
    ``Get-Credential``, confirmation prompts, ...).

    The hosted PowerShell runs with ``-NonInteractive``, so a prompt can never
    be answered: instead of hanging, the host fails the command immediately and
    virtualshell raises this error. Re-run the command with the value passed as
    a parameter (e.g. ``-Credential $cred``, ``-Confirm:$false``).
    """


class PolicyViolationError(VirtualShellError):
    """A command was blocked by the shell's :class:`~virtualshell.ExecutionPolicy`.

    Attributes
    ----------
    command : str
        The PowerShell text that was blocked (never executed).
    reason : str
        Human-readable explanation of which rule blocked it.
    matched : tuple[str, ...]
        The command names that triggered the rule.
    """

    def __init__(self, reason: str, *, command: str = "", matched: tuple = ()) -> None:
        super().__init__(reason)
        self.command = command
        self.reason = reason
        self.matched = tuple(matched)


class ScriptBlockDelegateWarning(UserWarning):
    """A command appears to convert a PowerShell scriptblock into a .NET
    delegate or event handler (``[SomeEventHandler]{ ... }`` casts,
    ``$obj.add_Event({ ... })``).

    Such handlers run whenever .NET fires the event — often on a thread that
    has no PowerShell runspace, which crashes the hidden host process
    (typically with a ``StackOverflowException``) and loses session state.
    The command still executes; this is a heads-up, not a block. Prefer a
    handler compiled in C# via ``Add-Type`` and attach that instead.
    """
