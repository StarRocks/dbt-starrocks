"""
Unit tests for connection helpers in dbt.adapters.starrocks.connections.
"""
import pytest

from dbt.adapters.starrocks.connections import _parse_version


DEFAULT = (999, 999, 999)


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Standard "<version>-<commit>" form returned by select current_version().
        ("4.0.7-b75f536", (4, 0, 7)),
        ("4.0.6-abc", (4, 0, 6)),
        ("4.0.5-x", (4, 0, 5)),
        ("4.1.0-rc01", (4, 1, 0)),
        ("3.5.14-x", (3, 5, 14)),
        ("4.0.10-x", (4, 0, 10)),
        ("3.5.17-deadbeef", (3, 5, 17)),
        ("4.0.7", (4, 0, 7)),
        ("8.0.33 (StarRocks)", (8, 0, 33)),
        # Anything that is not exactly three numeric parts falls back to the
        # sentinel default. 
        ("4.0", DEFAULT),
        ("3.5", DEFAULT),
        ("garbage", DEFAULT),
        ("4", DEFAULT),
        ("4.0.x", DEFAULT),
        ("", DEFAULT),
    ],
)
def test_parse_version(raw, expected):
    assert _parse_version(raw) == expected
