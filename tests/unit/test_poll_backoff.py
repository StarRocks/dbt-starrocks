import pytest

from dbt.adapters.starrocks.impl import StarRocksAdapter


class TestPollBackoffDefaults:
    """Verify default config matches original behavior (2^n capped at 600)."""

    @pytest.mark.parametrize("attempt,expected", [
        (1, 2.0),
        (2, 4.0),
        (3, 8.0),
        (4, 16.0),
        (5, 32.0),
        (9, 512.0),
        (10, 600.0),  # capped
        (15, 600.0),  # still capped
    ])
    def test_default_backoff(self, attempt, expected):
        delay = StarRocksAdapter._compute_poll_delay(attempt)
        assert delay == expected


class TestPollBackoffCustom:
    """Verify custom config produces expected delays."""

    poll_interval = 5
    poll_factor = 1.5
    poll_max_delay = 60

    @pytest.mark.parametrize("attempt,expected", [
        (1, 7.5),
        (2, 11.25),
        (3, 16.875),
        (4, 25.3125),
        (5, 37.96875),
        (6, 56.953125),
        (7, 60.0),  # capped
    ])
    def test_custom_backoff(self, attempt, expected):
        delay = StarRocksAdapter._compute_poll_delay(
            attempt, self.poll_interval, self.poll_factor, self.poll_max_delay
        )
        assert delay == expected

    def test_never_exceeds_max(self):
        for attempt in range(1, 100):
            delay = StarRocksAdapter._compute_poll_delay(
                attempt, self.poll_interval, self.poll_factor, self.poll_max_delay
            )
            assert delay <= self.poll_max_delay
