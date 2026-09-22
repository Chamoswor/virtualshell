"""Integration tests for PowerShell edition selection.

Runs against every edition installed on this machine (``edition`` fixture in
conftest.py): ``core`` = pwsh / PowerShell 7+, ``desktop`` = Windows
PowerShell 5.1. Skips itself without the compiled extension or a host.
"""
from __future__ import annotations

import os
import shutil

import pytest

from conftest import EDITIONS, integration

pytestmark = integration


@pytest.fixture(scope="module")
def shell(edition):
    from virtualshell import Shell

    sh = Shell(timeout=30, powershell_edition=edition).start()
    yield sh
    sh.stop(force=True)


class TestRequestedEdition:
    def test_running_host_matches_requested_edition(self, shell, edition):
        assert shell.configured_edition == edition
        assert shell.edition == edition
        reported = shell.run("$PSVersionTable.PSEdition").out.strip().lower()
        assert reported == edition

    def test_version_matches_edition(self, shell, edition):
        major = int(shell.powershell_version.split(".")[0])
        if edition == "core":
            assert major >= 7
        else:
            assert major == 5

    def test_resolved_path_names_the_right_executable(self, shell, edition):
        path = shell.powershell_path
        name = os.path.basename(path).lower()
        if edition == "core":
            assert name in ("pwsh", "pwsh.exe")
        else:
            assert name == "powershell.exe"
            assert "windowspowershell" in path.lower().replace("/", "\\")
        assert os.path.isabs(path)
        assert os.path.isfile(path)
        assert path.isprintable()

    def test_core_bindings_expose_edition_and_path(self, shell, edition):
        core = shell.get_module()
        assert core.get_powershell_edition() == edition
        assert core.get_resolved_powershell_path() == shell.powershell_path
        assert shell.get_config().powershell_edition == edition

    def test_edition_survives_timeout_restart(self, shell, edition):
        res = shell.run("Start-Sleep -Seconds 10", timeout=1)
        assert not res.success
        shell._wait_if_restarting()
        assert shell.run("1 + 1").out.strip() == "2"
        assert shell.run("$PSVersionTable.PSEdition").out.strip().lower() == edition


class TestAutoEdition:
    def test_auto_prefers_pwsh_then_windows_powershell(self):
        from virtualshell import Shell

        expected = "core" if "core" in EDITIONS else "desktop"
        with Shell(timeout=30) as sh:      # default powershell_edition="auto"
            assert sh.configured_edition == "auto"
            assert sh.edition == expected

    def test_explicit_path_overrides_edition(self):
        from virtualshell import Shell

        pwsh = shutil.which("pwsh")
        if not pwsh or "desktop" not in EDITIONS:
            pytest.skip("needs both pwsh and Windows PowerShell installed")
        with Shell(timeout=30, powershell_path=pwsh,
                   powershell_edition="desktop") as sh:
            assert sh.edition == "core"
            assert os.path.normcase(sh.powershell_path) == os.path.normcase(pwsh)

    def test_desktop_request_off_windows_is_rejected_early(self):
        from virtualshell import Shell

        if os.name == "nt":
            pytest.skip("Windows PowerShell exists here")
        with pytest.raises(ValueError):
            Shell(powershell_edition="desktop")
