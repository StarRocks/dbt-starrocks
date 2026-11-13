from types import SimpleNamespace

import pytest

from dbt.adapters.starrocks.impl import StarRocksAdapter


class LazyConnection:
    """Mimics a dbt Connection whose handle is still a LazyHandle: open() only
    runs, and sets starrocks_version, when handle is first read."""

    def __init__(self, version):
        self._version = version

    @property
    def handle(self):
        if self._version is not None:
            self.starrocks_version = self._version
        return object()


def _adapter(conn):
    adapter = StarRocksAdapter.__new__(StarRocksAdapter)
    adapter.connections = SimpleNamespace(get_if_exists=lambda: conn)
    return adapter


class TestIsBeforeVersion:
    @pytest.mark.parametrize("server,gate,expected", [
        ((3, 3, 22), "3.4.0", True),
        ((3, 4, 0), "3.4.0", False),
        ((3, 5, 14), "3.4.0", False),
        ((3, 0, 9), "3.1.0", True),
        ((4, 0, 10), "4.1.0", True),
        ((4, 1, 0), "4.1.0", False),
    ])
    def test_compares_against_starrocks_version(self, server, gate, expected):
        assert _adapter(LazyConnection(server)).is_before_version(gate) is expected

    def test_opens_lazy_connection_before_reading_version(self):
        # The gate can render before any SQL has run on the thread's connection.
        assert _adapter(LazyConnection((3, 0, 0))).is_before_version("3.1.0") is True

    def test_unknown_version_is_not_before(self):
        assert _adapter(LazyConnection(None)).is_before_version("3.1.0") is False

    def test_no_connection_is_not_before(self):
        assert _adapter(None).is_before_version("3.1.0") is False


class TestCurrentVersion:
    def test_formats_starrocks_version(self):
        assert _adapter(LazyConnection((3, 3, 22))).current_version() == "3.3.22"

    @pytest.mark.parametrize("conn", [
        LazyConnection(None),
        LazyConnection((999, 999, 999)),
        None,
    ])
    def test_unknown(self, conn):
        assert _adapter(conn).current_version() == "UNKNOWN"
