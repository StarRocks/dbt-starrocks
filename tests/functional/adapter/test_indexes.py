import pytest
from dbt.tests.util import run_dbt


single_index_model = """
{{ config(
    materialized='table',
    engine='OLAP',
    distributed_by=['id'],
    indexs=[{'columns': 'name'}]
) }}
select 1 as id, 'test' as name
"""

multiple_indexes_model = """
{{ config(
    materialized='table',
    engine='OLAP',
    distributed_by=['id'],
    indexs=[
        {'columns': 'name'},
        {'columns': 'email'},
        {'columns': 'status'}
    ]
) }}
select 1 as id, 'test' as name, 'test@example.com' as email, 'active' as status
"""

multiple_columns_index_model = """
{{ config(
    materialized='table',
    engine='OLAP',
    distributed_by=['id'],
    indexs=[{'columns': 'name, email'}]
) }}
select 1 as id, 'test' as name, 'test@example.com' as email
"""

class TestSingleIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {"single_index.sql": single_index_model}

    def test_single_index(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        
        relation = results[0].node.relation_name
        result = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "INDEX idx_name" in result
        assert "USING BITMAP" in result


class TestMultipleIndexes:
    @pytest.fixture(scope="class")
    def models(self):
        return {"multiple_indexes.sql": multiple_indexes_model}

    def test_multiple_indexes(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        
        relation = results[0].node.relation_name
        result = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "INDEX idx_name" in result
        assert "INDEX idx_email" in result
        assert "INDEX idx_status" in result
        assert result.count("USING BITMAP") == 3


class TestMultipleColumnsIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {"multiple_columns_index.sql": multiple_columns_index_model}

    def test_multiple_columns_index(self, project):
        with pytest.raises(AssertionError):
            run_dbt(["run"])
