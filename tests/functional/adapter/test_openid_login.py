"""Log in through a StarRocks JWT security integration with a real ID token.

Needs the stack in .github/docker-compose.openid.yml; skipped without it, so the
regular functional run collects and skips it.
"""
import json
import os
import urllib.parse
import urllib.request

import mysql.connector
import pytest
from dbt.tests.util import run_dbt

TOKEN_URL = os.getenv("OPENID_TOKEN_URL")

pytestmark = pytest.mark.skipif(
    not TOKEN_URL, reason="OPENID_TOKEN_URL not set; run via docker-compose.openid.yml"
)

USERNAME = os.getenv("OPENID_USERNAME", "dbt_user")
HOST = os.getenv("STARROCKS_HOST", "starrocks")
PORT = int(os.getenv("STARROCKS_PORT", 9030))
CA = os.getenv("STARROCKS_CA")


def _fetch_id_token():
    body = urllib.parse.urlencode({
        "grant_type": "password",
        "client_id": os.getenv("OPENID_CLIENT_ID", "dbt-starrocks"),
        "username": USERNAME,
        "password": os.environ["OPENID_PASSWORD"],
        "scope": "openid",
    }).encode()
    with urllib.request.urlopen(TOKEN_URL, data=body, timeout=30) as response:
        return json.load(response)["id_token"]


@pytest.fixture(scope="session")
def id_token_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("openid") / "id_token.jwt"
    path.write_text(_fetch_id_token())
    return str(path)


@pytest.fixture(scope="class")
def dbt_profile_target(id_token_file):
    return {
        "type": "starrocks",
        "host": HOST,
        "port": PORT,
        "username": USERNAME,
        "password": "",
        "auth_plugin": "authentication_openid_connect_client",
        "openid_token_file": id_token_file,
        "ssl_ca": CA,
        "ssl_verify_cert": True,
        "ssl_verify_identity": True,
        "use_pure": "true",
    }


class TestOpenIDLogin:
    @pytest.fixture(scope="class")
    def models(self):
        return {"openid_model.sql": "{{ config(materialized='table') }} select 1 as id"}

    def test_dbt_run_as_token_user(self, project):
        user = project.run_sql("select current_user()", fetch="one")[0]
        assert USERNAME in user, f"connected as {user}, not the token's user"

        results = run_dbt(["run"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select id from {project.test_schema}.openid_model", fetch="all"
        )
        assert rows == [(1,)]


def test_tampered_token_is_rejected(id_token_file, tmp_path):
    header, payload, signature = open(id_token_file).read().strip().split(".")
    flipped = ("A" if signature[0] != "A" else "B") + signature[1:]
    bad_token = tmp_path / "bad.jwt"
    bad_token.write_text(".".join([header, payload, flipped]))

    with pytest.raises(mysql.connector.Error, match="Access denied"):
        mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USERNAME,
            auth_plugin="authentication_openid_connect_client",
            openid_token_file=str(bad_token),
            ssl_ca=CA,
            ssl_verify_cert=True,
            use_pure=True,
        )
