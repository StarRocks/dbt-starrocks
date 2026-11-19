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


class _Handle(SimpleNamespace):
    def cursor(self):
        return SimpleNamespace(execute=lambda sql: None)


def _open(monkeypatch, fail_first=False, **profile):
    calls = []

    def connect(**kwargs):
        calls.append(dict(kwargs))
        if fail_first and len(calls) == 1:
            raise connections.mysql.connector.Error("database does not exist")
        return _Handle()

    monkeypatch.setattr(connections.mysql.connector, "connect", connect)
    credentials = StarRocksCredentials(
        host="localhost", port=9030, schema="analytics", username="alice",
        password="", version="4.1.7", **profile,
    )
    StarRocksConnectionManager.open(_Connection(credentials))
    return calls


OIDC = {
    "auth_plugin": "authentication_openid_connect_client",
    "openid_token_file": "/tmp/id_token.jwt",
}


def test_openid_token_file_is_passed_to_the_connector(monkeypatch):
    (kwargs,) = _open(monkeypatch, **OIDC)
    assert kwargs["auth_plugin"] == "authentication_openid_connect_client"
    assert kwargs["openid_token_file"] == "/tmp/id_token.jwt"


def test_openid_token_file_is_passed_on_the_create_database_retry(monkeypatch):
    calls = _open(monkeypatch, fail_first=True, **OIDC)
    assert len(calls) == 3
    assert all(c["openid_token_file"] == "/tmp/id_token.jwt" for c in calls)


def test_openid_token_file_is_omitted_when_unset(monkeypatch):
    (kwargs,) = _open(monkeypatch)
    assert "openid_token_file" not in kwargs


def test_ssl_options_are_passed_when_set(monkeypatch):
    (kwargs,) = _open(
        monkeypatch,
        ssl_ca="/etc/ssl/certs/ca-certificates.crt",
        ssl_verify_cert=True,
        ssl_verify_identity=False,
        **OIDC,
    )
    assert kwargs["ssl_ca"] == "/etc/ssl/certs/ca-certificates.crt"
    assert kwargs["ssl_verify_cert"] is True
    assert kwargs["ssl_verify_identity"] is False


def test_ssl_options_are_omitted_when_unset(monkeypatch):
    (kwargs,) = _open(monkeypatch, **OIDC)
    assert not {"ssl_ca", "ssl_verify_cert", "ssl_verify_identity"} & kwargs.keys()
