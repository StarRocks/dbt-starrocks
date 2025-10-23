import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'starrocks',
#        'threads': 1,
#        'server': 'localhost',
        'username': 'root',
        'password': '',
        'port': 9030,
        'host': 'localhost',
#        'version': '3.0',
#        'schema': 'dbt',
#        'ssl_disabled': True
    }

def pytest_addoption(parser):
    parser.addoption(
        "--runexternal", action="store_true", default=False, help="run external tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "external: mark test as external to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runexternal"):
        # --runexternal given in cli: do not skip external tests
        return
    skip_external = pytest.mark.skip(reason="need --runexternal option to run")
    for item in items:
        if "external" in item.keywords:
            item.add_marker(skip_external)