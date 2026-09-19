"""Unit tests for the zero-copy channel protocol (no PowerShell needed).

Two _MmapChannel instances share one backing file: the "python" side plays
its normal role while a second instance plays the PowerShell peer's role in
a thread, exercising both directions of protocol v3 cross-platform.
"""
from __future__ import annotations

import struct
import threading

import pytest

from virtualshell.zero_copy_bridge_shell import (
    _BLK_PY2PS,
    _F_ERR,
    _F_READY,
    _MmapChannel,
)

FRAME = 256 * 1024  # small frame keeps test files tiny
CHUNK = 64 * 1024


@pytest.fixture
def channel_pair(tmp_path):
    path = tmp_path / "chan.bin"
    owner = _MmapChannel(path, FRAME, create=True)
    peer = _MmapChannel(path, create=False)
    yield owner, peer
    peer.close()
    owner.unlink()


def _run_peer(fn):
    """Run the peer role in a thread; return a handle exposing the result."""
    box: dict = {}

    def target():
        try:
            box["result"] = fn()
        except Exception as exc:  # surfaced by join()
            box["error"] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()

    class Handle:
        @staticmethod
        def join(timeout=15.0):
            t.join(timeout)
            assert not t.is_alive(), "peer thread hung"
            if "error" in box:
                raise box["error"]
            return box.get("result")

    return Handle


class TestChannelCreation:
    def test_header_written_and_reopened(self, tmp_path):
        path = tmp_path / "hdr.bin"
        owner = _MmapChannel(path, FRAME, create=True)
        try:
            peer = _MmapChannel(path, create=False)
            assert peer.frame_bytes == FRAME
            peer.close()
        finally:
            owner.unlink()
        assert not path.exists()

    def test_rejects_foreign_file(self, tmp_path):
        path = tmp_path / "bogus.bin"
        path.write_bytes(b"\x00" * 1024)
        with pytest.raises(ValueError, match="Not a virtualshell channel"):
            _MmapChannel(path, create=False)

    def test_create_requires_frame_size(self, tmp_path):
        with pytest.raises(ValueError):
            _MmapChannel(tmp_path / "x.bin", 0, create=True)


class TestPy2Ps:
    def test_single_chunk(self, channel_pair):
        owner, peer = channel_pair
        payload = b"hello powershell"
        owner.reset("py2ps")
        reader = _run_peer(lambda: peer.receive(timeout=10, direction="py2ps"))
        owner.send(payload, chunk_size=CHUNK, timeout=10, direction="py2ps")
        assert bytes(reader.join()) == payload

    def test_multi_chunk_reassembly(self, channel_pair):
        owner, peer = channel_pair
        payload = bytes(range(256)) * 1024  # 256 KiB -> 4 chunks of 64 KiB
        owner.reset("py2ps")
        reader = _run_peer(lambda: peer.receive(timeout=10, direction="py2ps"))
        owner.send(payload, chunk_size=CHUNK, timeout=10, direction="py2ps")
        assert bytes(reader.join()) == payload

    def test_empty_payload(self, channel_pair):
        owner, peer = channel_pair
        owner.reset("py2ps")
        reader = _run_peer(lambda: peer.receive(timeout=10, direction="py2ps"))
        owner.send(b"", chunk_size=CHUNK, timeout=10, direction="py2ps")
        assert bytes(reader.join()) == b""

    def test_sequential_transfers_reuse_channel(self, channel_pair):
        owner, peer = channel_pair
        for payload in (b"first", b"second-longer-payload", b""):
            # The contract requires clearing stale flags from the previous
            # transfer before the reader starts (the bridge always does this).
            owner.reset("py2ps")
            reader = _run_peer(lambda: peer.receive(timeout=10, direction="py2ps"))
            owner.send(payload, chunk_size=CHUNK, timeout=10, direction="py2ps")
            assert bytes(reader.join()) == payload

    def test_chunk_size_clamped_to_frame(self, channel_pair):
        owner, peer = channel_pair
        payload = b"x" * (FRAME + 100)  # forces >1 chunk even at max chunk size
        owner.reset("py2ps")
        reader = _run_peer(lambda: peer.receive(timeout=10, direction="py2ps"))
        owner.send(payload, chunk_size=FRAME * 10, timeout=10, direction="py2ps")
        assert bytes(reader.join()) == payload


class TestPs2Py:
    def test_round_trip(self, channel_pair):
        owner, peer = channel_pair
        payload = b"fra powershell \xc3\xa6\xc3\xb8\xc3\xa5"
        owner.reset("ps2py")
        writer = _run_peer(
            lambda: peer.send(payload, chunk_size=CHUNK, timeout=10, direction="ps2py"))
        received = owner.receive(timeout=10, direction="ps2py")
        writer.join()
        assert bytes(received) == payload

    def test_multi_chunk(self, channel_pair):
        owner, peer = channel_pair
        payload = b"\xab" * (3 * CHUNK + 17)
        owner.reset("ps2py")
        writer = _run_peer(
            lambda: peer.send(payload, chunk_size=CHUNK, timeout=10, direction="ps2py"))
        received = owner.receive(timeout=10, direction="ps2py")
        writer.join()
        assert bytes(received) == payload

    def test_reset_clears_stale_flags(self, channel_pair):
        owner, peer = channel_pair
        # Complete one transfer, leaving done=1 in the ps2py block.
        owner.reset("ps2py")
        writer = _run_peer(
            lambda: peer.send(b"old", chunk_size=CHUNK, timeout=10, direction="ps2py"))
        owner.receive(timeout=10, direction="ps2py")
        writer.join()

        # Without reset, a new receive would instantly see the stale done flag
        # and return empty. With reset it must wait for the fresh transfer.
        owner.reset("ps2py")
        writer = _run_peer(
            lambda: peer.send(b"new-data", chunk_size=CHUNK, timeout=10, direction="ps2py"))
        assert bytes(owner.receive(timeout=10, direction="ps2py")) == b"new-data"
        writer.join()


class TestFailureModes:
    def test_receive_timeout(self, channel_pair):
        owner, _ = channel_pair
        owner.reset("ps2py")
        with pytest.raises(TimeoutError):
            owner.receive(timeout=0.2, direction="ps2py")

    def test_send_timeout_without_reader(self, channel_pair):
        owner, _ = channel_pair
        with pytest.raises(TimeoutError):
            owner.send(b"nobody listens", chunk_size=CHUNK, timeout=0.2,
                       direction="py2ps")

    def test_peer_abort_propagates(self, channel_pair):
        owner, peer = channel_pair
        owner.reset("py2ps")

        # Simulate the PowerShell side aborting mid-transfer: once the first
        # chunk is offered (ready==1), write the PS abort code instead of acking.
        def abort_when_ready():
            import time
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if struct.unpack_from("<i", peer._mm, _BLK_PY2PS + _F_READY)[0] == 1:
                    struct.pack_into("<i", peer._mm, _BLK_PY2PS + _F_ERR, 2)
                    return
                time.sleep(0.001)

        aborter = _run_peer(abort_when_ready)
        with pytest.raises(RuntimeError, match="Peer aborted"):
            owner.send(b"data", chunk_size=CHUNK, timeout=5, direction="py2ps")
        aborter.join()

    def test_closed_channel_file_removed_despite_open_peer(self, tmp_path):
        # unlink() retries while the peer still holds a mapping; once the peer
        # closes, the file must eventually disappear (best-effort on Windows).
        path = tmp_path / "gone.bin"
        owner = _MmapChannel(path, FRAME, create=True)
        peer = _MmapChannel(path, create=False)
        peer.close()
        owner.unlink()
        assert not path.exists()
