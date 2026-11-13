from types import SimpleNamespace

from dbt.adapters.starrocks import connections
from dbt.adapters.starrocks.connections import (
    StarRocksConnectionManager,
    StarRocksCredentials,
)


class _Connection:
    def __init__(self, credentials):
        self.state = "init"
        self.credentials = credentials
        self.handle = None


def _open(monkeypatch, **profile):
    calls = []
    monkeypatch.setattr(
        connections.mysql.connector, "connect", lambda **kwargs: calls.append(kwargs) or SimpleNamespace()
    )
    credentials = StarRocksCredentials(
        host="localhost", port=9030, schema="analytics", username="alice",
        password="", version="4.1.7", **profile,
    )
    StarRocksConnectionManager.open(_Connection(credentials))
    return calls[0]


def test_openid_token_file_is_passed_to_the_connector(monkeypatch):
    kwargs = _open(
        monkeypatch,
        auth_plugin="authentication_openid_connect_client",
        openid_token_file="/tmp/id_token.jwt",
    )
    assert kwargs["auth_plugin"] == "authentication_openid_connect_client"
    assert kwargs["openid_token_file"] == "/tmp/id_token.jwt"


def test_openid_token_file_is_omitted_when_unset(monkeypatch):
    assert "openid_token_file" not in _open(monkeypatch)
