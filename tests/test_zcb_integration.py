"""Integration tests for the cross-platform ZeroCopyBridge against a real host.

Requires the compiled `virtualshell._core` extension and pwsh and/or Windows
PowerShell 5.1; runs once per installed edition and skips itself otherwise
(same policy as test_integration.py).
"""
from __future__ import annotations

import hashlib
import os

import pytest

from virtualshell.ps_object import PSObject

from conftest import integration

pytestmark = integration


@pytest.fixture(scope="module")
def shell(edition):
    from virtualshell import Shell

    sh = Shell(timeout_seconds=60, powershell_edition=edition).start()
    yield sh
    sh.stop(force=True)


@pytest.fixture(scope="module")
def bridge(shell):
    from virtualshell import ZeroCopyBridge

    br = ZeroCopyBridge(shell, frame_mb=8, chunk_mb=1)
    yield br
    br.close()


class TestPythonToPowerShell:
    def test_bytes_arrive_as_byte_array(self, shell, bridge):
        payload = bytes(range(256))
        bridge.send(payload, "vs_small")
        assert shell.run("$vs_small.GetType().Name").out.strip() == "Byte[]"
        assert shell.run("$vs_small.Length").out.strip() == "256"
        assert shell.run("$vs_small[255]").out.strip() == "255"

    def test_large_multi_chunk_content_integrity(self, shell, bridge):
        payload = os.urandom(3 * 1024 * 1024 + 12345)  # > 3 chunks at 1 MB
        bridge.send(payload, "vs_big")
        ps_hash = shell.run(
            "(Get-FileHash -InputStream ([IO.MemoryStream]::new($vs_big)) "
            "-Algorithm SHA256).Hash",
            timeout=60,
        ).out.strip().lower()
        assert ps_hash == hashlib.sha256(payload).hexdigest()

    def test_empty_payload(self, shell, bridge):
        bridge.send(b"", "vs_empty")
        assert shell.run("$vs_empty.Length").out.strip() == "0"

    def test_utf8_text_round_trip_via_powershell(self, shell, bridge):
        text = "hei fra python: æøå ✓"
        bridge.send(text.encode("utf-8"), "vs_text")
        res = shell.run("[Text.Encoding]::UTF8.GetString($vs_text)")
        assert res.out.strip() == text

    def test_send_then_deserialize_object(self, shell, bridge):
        obj = PSObject("System.Management.Automation.PSCustomObject", [
            PSObject.Property("Name", str, "fra-python"),
            PSObject.Property("Count", int, 3),
        ])
        bridge.send(obj.to_bytes(), "vs_pyobj")
        assert bridge.deserialize("vs_pyobj") is True
        assert shell.run("$vs_pyobj.Name").out.strip() == "fra-python"
        assert shell.run("$vs_pyobj.Count").out.strip() == "3"


class TestPowerShellToPython:
    def test_byte_array_round_trip(self, shell, bridge):
        shell.run("$vs_out = [byte[]](0..255)", raise_on_error=True)
        data = bridge.receive("vs_out")
        assert data == bytes(range(256))

    def test_large_multi_chunk(self, shell, bridge):
        shell.run(
            "$vs_rand = [byte[]]::new(3145728); "
            "[System.Random]::new(42).NextBytes($vs_rand)",
            raise_on_error=True, timeout=60,
        )
        data = bridge.receive("vs_rand", timeout=60)
        ps_hash = shell.run(
            "(Get-FileHash -InputStream ([IO.MemoryStream]::new($vs_rand)) "
            "-Algorithm SHA256).Hash",
            timeout=60,
        ).out.strip().lower()
        assert len(data) == 3145728
        assert hashlib.sha256(data).hexdigest() == ps_hash

    def test_receive_returns_memoryview_when_asked(self, shell, bridge):
        shell.run("$vs_mv = [byte[]](1,2,3)", raise_on_error=True)
        view = bridge.receive("vs_mv", return_memoryview=True)
        assert isinstance(view, memoryview)
        assert bytes(view) == b"\x01\x02\x03"

    def test_non_byte_variable_is_serialized_to_clixml(self, bridge, shell):
        shell.run(
            "$vs_obj = [PSCustomObject]@{Name='bridge'; Id=7; Ok=$true}",
            raise_on_error=True,
        )
        data = bridge.receive("vs_obj")
        obj = PSObject.from_bytes(bytes(data))
        assert obj["Name"] == "bridge"
        assert obj["Id"] == 7
        assert obj["Ok"] is True

    def test_serialize_helper_then_receive(self, shell, bridge):
        shell.run(
            "$vs_ser = [PSCustomObject]@{Inner=[PSCustomObject]@{Deep=1}; Top='x'}",
            raise_on_error=True,
        )
        assert bridge.serialize("vs_ser", depth=3) is True
        assert shell.run("$vs_ser.GetType().Name").out.strip() == "Byte[]"
        obj = PSObject.from_bytes(bytes(bridge.receive("vs_ser")))
        assert obj["Top"] == "x"
        assert obj["Inner"]["Deep"] == 1

    def test_round_trip_python_ps_python(self, shell, bridge):
        payload = os.urandom(200_000)
        bridge.send(payload, "vs_echo")
        assert bridge.receive("vs_echo") == payload


class TestLifecycle:
    def test_close_releases_and_deletes_channel_file(self, shell):
        from virtualshell import ZeroCopyBridge

        br = ZeroCopyBridge(shell, frame_mb=1, chunk_mb=1)
        path = br.channel_path
        assert path.exists()
        br.send(b"quick", "vs_lc")
        br.close()
        assert not path.exists()

    def test_operations_after_close_raise(self, shell):
        from virtualshell import ZeroCopyBridge

        br = ZeroCopyBridge(shell, frame_mb=1, chunk_mb=1)
        br.close()
        with pytest.raises(RuntimeError, match="closed"):
            br.send(b"x", "vs_after")
        with pytest.raises(RuntimeError, match="closed"):
            br.receive("vs_after")

    def test_context_manager(self, shell):
        from virtualshell import ZeroCopyBridge

        with ZeroCopyBridge(shell, frame_mb=1, chunk_mb=1) as br:
            br.send(b"ctx", "vs_ctx")
            path = br.channel_path
        assert not path.exists()
