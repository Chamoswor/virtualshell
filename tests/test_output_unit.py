"""Unit tests for the output-budget machinery (virtualshell.output) and its
wiring into Shell.run / Shell.fetch_output, run against the fake core."""
from __future__ import annotations

import pytest

from virtualshell.output import OutputSlice, OutputStore, apply_budget
from virtualshell.shell import Shell
from virtualshell.errors import VirtualShellError

from conftest import FakeExecutionResult


def make_text(lines: int) -> str:
    return "\n".join(f"line {i}" for i in range(1, lines + 1))


class TestOutputStore:
    def test_put_get_roundtrip(self):
        store = OutputStore()
        key = store.put("hello")
        assert store.get(key) == "hello"
        assert key in store

    def test_eviction_by_entries(self):
        store = OutputStore(max_entries=2)
        k1 = store.put("a")
        k2 = store.put("b")
        k3 = store.put("c")
        assert k1 not in store
        assert store.get(k2) == "b"
        assert store.get(k3) == "c"

    def test_eviction_by_size(self):
        store = OutputStore(max_entries=100, max_total_chars=10)
        k1 = store.put("aaaaaa")
        k2 = store.put("bbbbbb")  # 12 chars total -> k1 evicted
        assert k1 not in store
        assert k2 in store

    def test_missing_key_explains_eviction(self):
        store = OutputStore()
        with pytest.raises(KeyError, match="evicted or never existed"):
            store.get("nope")

    def test_slice_paging(self):
        store = OutputStore()
        key = store.put("0123456789")
        s1 = store.slice(key, offset=0, size=4)
        assert (s1.text, s1.next_offset, s1.total_chars) == ("0123", 4, 10)
        assert not s1.at_end
        s2 = store.slice(key, offset=s1.next_offset, size=100)
        assert s2.text == "456789"
        assert s2.at_end and s2.next_offset is None

    def test_slice_size_zero_means_everything(self):
        store = OutputStore()
        key = store.put("abc")
        assert store.slice(key, size=0).text == "abc"


class TestApplyBudget:
    def test_small_text_untouched(self):
        store = OutputStore()
        text, key = apply_budget("short", 100, store)
        assert text == "short" and key is None
        assert len(store) == 0  # nothing stored when nothing truncated

    def test_truncated_within_budget_with_marker(self):
        store = OutputStore()
        original = make_text(500)
        text, key = apply_budget(original, 1000, store)
        assert key is not None
        assert len(text) <= 1000
        assert "virtualshell: output truncated" in text
        assert f"fetch_output('{key}'" in text
        # Head and tail come from the original.
        assert text.startswith("line 1\n")
        assert text.rstrip().endswith(f"line 500")
        # The full text is retrievable.
        assert store.get(key) == original

    def test_marker_reports_totals(self):
        store = OutputStore()
        original = make_text(500)
        text, _ = apply_budget(original, 1000, store)
        assert f"{len(original)} chars / 500 lines total" in text

    def test_tiny_budget_still_bounded(self):
        store = OutputStore()
        text, key = apply_budget("x" * 5000, 120, store)
        assert key is not None
        assert len(text) <= 120
        assert "fetch_output(" in text

    def test_zero_budget_means_unlimited(self):
        store = OutputStore()
        text, key = apply_budget("x" * 5000, 0, store)
        assert len(text) == 5000 and key is None


class TestShellBudgetWiring:
    def test_run_applies_per_call_budget(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out="y" * 9000)
        res = sh.run("big", max_output=500)
        assert len(res.out) <= 500
        assert "fetch_output(" in res.out

    def test_run_applies_session_default_budget(self, fake_core):
        sh = Shell(max_output=400, cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out="y" * 9000)
        res = sh.run("big")
        assert len(res.out) <= 400

    def test_per_call_budget_overrides_session(self, fake_core):
        sh = Shell(max_output=400, cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out="y" * 9000)
        assert len(sh.run("big", max_output=800).out) <= 800
        assert len(sh.run("big", max_output=0).out) == 9000  # 0 = unlimited

    def test_batch_budgeted_per_result(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out=cmd * 3000)
        results = sh.run(["a", "b"], max_output=300)
        assert all(len(r.out) <= 300 for r in results)

    def test_fetch_output_pages_through(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        full = make_text(1000)
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out=full)
        res = sh.run("big", max_output=600)
        key = res.out.split("fetch_output('")[1].split("'")[0]

        collected, offset = "", 0
        while True:
            page = sh.fetch_output(key, offset=offset, max_output=2000)
            collected += page.text
            if page.next_offset is None:
                break
            offset = page.next_offset
        assert collected == full

    def test_budget_annotates_result_fields(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out="y" * 9000)
        res = sh.run("big", max_output=500)
        assert res.truncated is True
        assert res.output_key
        assert res.error_key is None
        assert sh.fetch_output(res.output_key).total_chars == 9000

    def test_error_stream_gets_its_own_key(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(
            out="ok", err="e" * 9000, exit_code=1, success=False)
        res = sh.run("noisy", max_output=500)
        assert res.truncated is True
        assert res.output_key is None          # out fit the budget
        assert res.error_key
        assert sh.fetch_output(res.error_key).total_chars == 9000

    def test_untruncated_result_fields_are_falsy(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        res = sh.run("small")                  # no budget at all
        assert res.truncated is False
        assert res.output_key is None and res.error_key is None

    def test_fetch_output_unknown_key_raises_virtualshell_error(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        with pytest.raises(VirtualShellError, match="evicted or never existed"):
            sh.fetch_output("deadbeef")

    def test_stop_clears_store(self, fake_core):
        sh = Shell(cpp_module=fake_core).start()
        fake_core.last_shell.result_factory = lambda cmd: FakeExecutionResult(out="y" * 9000)
        res = sh.run("big", max_output=500)
        key = res.out.split("fetch_output('")[1].split("'")[0]
        sh.stop()
        with pytest.raises(VirtualShellError):
            sh.fetch_output(key)
