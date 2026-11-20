from types import SimpleNamespace
from unittest import mock

import pytest
import dbt.exceptions
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.starrocks import impl
from dbt.adapters.starrocks.impl import StarRocksAdapter


def _adapter():
    """A StarRocksAdapter with only what _poll_for_complete_task touches."""
    adapter = object.__new__(StarRocksAdapter)
    adapter.connections = mock.Mock()
    adapter.config = SimpleNamespace(
        credentials=SimpleNamespace(poll_interval=0, poll_factor=1.0, poll_max_delay=0)
    )
    return adapter


def _poll(states):
    """
    Poll with task_runs returning one entry of `states` per query: None for no
    row, otherwise the STATE value. Returns (result, number of queries).
    """
    responses = iter(states)
    calls = 0

    def fake_execute(self, sql, fetch=False, limit=None, auto_begin=False):
        nonlocal calls
        calls += 1
        state = next(responses)
        rows = [] if state is None else [{"STATE": state, "PROGRESS": "0%"}]
        return SimpleNamespace(code="SUCCESS"), rows

    with mock.patch.object(SQLAdapter, "execute", fake_execute), \
            mock.patch.object(impl.time, "sleep"):
        result = _adapter()._poll_for_complete_task("t1")
    return result, calls


class TestPollMissingRun:
    def test_missing_between_pending_and_running_keeps_polling(self):
        # Observed on 3.5: PENDING -> (missing) -> RUNNING -> SUCCESS.
        (_, rows), calls = _poll(["PENDING", None, "RUNNING", "SUCCESS"])
        assert rows[0]["STATE"] == "SUCCESS"
        assert calls == 4

    def test_missing_on_first_poll_keeps_polling(self):
        (_, rows), calls = _poll([None, None, "SUCCESS"])
        assert rows[0]["STATE"] == "SUCCESS"
        assert calls == 3

    def test_missing_counter_resets_once_run_reappears(self):
        gap = [None] * impl.MISSING_RUN_MAX_RETRIES
        (_, rows), _ = _poll(gap + ["RUNNING"] + gap + ["SUCCESS"])
        assert rows[0]["STATE"] == "SUCCESS"

    def test_persistently_missing_run_raises(self):
        states = [None] * (impl.MISSING_RUN_MAX_RETRIES + 1)
        with pytest.raises(dbt.exceptions.DbtRuntimeError, match="was not found"):
            _poll(states)
