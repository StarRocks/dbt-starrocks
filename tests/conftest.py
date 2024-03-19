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
