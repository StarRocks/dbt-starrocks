import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.util import (
    check_relation_types,
    relation_from_name,
    run_dbt,
)

seed_base_csv = """
id,name,some_date
1,Alice,2023-01-01
2,Bob,2023-01-02
3,Charlie,2023-01-03
4,David,2023-01-04
5,Eve,2023-01-05
6,Frank,2023-01-06
7,Grace,2023-01-07
8,Henry,2023-01-08
9,Iris,2023-01-09
10,Jack,2023-01-10
""".lstrip()

external_catalog_table_sql = """
{{
    config(
        materialized = 'table',
        catalog = 'iceberg_catalog',
        database = 'dbt_test_db',
        partition_by = ['some_date'], 
    )
}}
select * from {{ ref('base') }}
""".lstrip()


class TestExternalCatalogTable:
    """Test basic table materialization in external catalog"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seed_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "external_table.sql": external_catalog_table_sql,
        }
    
    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        """Create external database with location"""
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_test_db
            PROPERTIES ("location" = "<s3_location>")
        """)
        yield

        project.run_sql("DROP TABLE IF EXISTS iceberg_catalog.dbt_test_db.external_table")
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_test_db FORCE")

    def test_external_catalog_table(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
    
        results = run_dbt()
        assert len(results) == 1
        
        relation = relation_from_name(project.adapter, "external_table")
        result = project.run_sql(
            f"select count(*) as num_rows from iceberg_catalog.dbt_test_db.{relation.identifier}", 
            fetch="one"
        )
        assert result[0] == 10
        
        # Verify it's actually a table
        expected = {
            "base": "table",
            "external_table": "table",
        }
        check_relation_types(project.adapter, expected)