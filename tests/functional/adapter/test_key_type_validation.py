import pytest

from dbt.tests.util import run_dbt

invalid_model_sql = """
{{ config(
    materialized='table',
    table_type='DUPLICATE',
    keys=['hk_col'],
    distributed_by=['hk_col'],
    engine='OLAP'
) }}
select
    cast(x'010121' as varbinary(16)) as hk_col
""".lstrip()

valid_model_sql = """
{{ config(
    materialized='table',
    table_type='DUPLICATE',
    keys=['bigint_col'],
    distributed_by=['bigint_col'],
    engine='OLAP'
) }}
select
    cast(123 as bigint) as bigint_col
""".lstrip()

@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'starrocks',
        'username': 'root',
        'password': '',
        'port': 9030,
        'host': 'localhost',
        'is_async': True,
        'async_query_timeout': 10,
    }

class TestBinaryKeyValidation:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "binary_key_model.sql": invalid_model_sql,
            "valid_key_model.sql": valid_model_sql,
        }

    def test_invalid_key_succeeds(self):
        """
        Expected behavior:
        - dbt run FAILS with DbtRuntimeError
        """
        with pytest.raises(AssertionError):
            run_dbt(["run", "--select", "binary_key_model"])

    def test_valid_key_succeeds(self):
        """
        Expected behavior:
        - dbt run SUCCEEDS and table is created
        """
        results = run_dbt(["run", "--select", "valid_key_model"])
        assert len(results) == 1
