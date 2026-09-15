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
            PROPERTIES ("location" = "s3://warehouse/dbt_test_db")
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


seed_partition_1_csv = """
id,partition_key
1,1
2,1
3,1
4,1
5,1
""".lstrip()

seed_partition_1_replaced_plus_2_csv = """
id,partition_key
6,1
7,1
8,1
9,1
10,1
11,2
12,2
13,2
14,2
15,2
""".lstrip()

external_catalog_incremental_sql = """
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'dynamic_overwrite',
        catalog = 'iceberg_catalog',
        database = 'dbt_test_db',
        partition_by = ['partition_key'],
    )
}}
select id, partition_key from {{ ref(var('seed_name')) }}
""".lstrip()


class TestExternalCatalogIncremental:
    """Test incremental materialization in an external catalog.

    A second run has to find the table it created on the first one. Relations in
    an external catalog were invisible to the relation cache, so every run after
    the first re-emitted CREATE TABLE and failed with "table already exists".
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "partition_1.csv": seed_partition_1_csv,
            "partition_1_replaced_plus_2.csv": seed_partition_1_replaced_plus_2_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_external.sql": external_catalog_incremental_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # A default so the model parses before the first --vars override.
        return {"vars": {"seed_name": "partition_1"}}

    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_test_db
            PROPERTIES ("location" = "s3://warehouse/dbt_test_db")
        """)
        yield

        project.run_sql("DROP TABLE IF EXISTS iceberg_catalog.dbt_test_db.incremental_external")
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_test_db FORCE")

    @staticmethod
    def _ids_by_partition(project):
        rows = project.run_sql(
            "select partition_key, id"
            " from iceberg_catalog.dbt_test_db.incremental_external"
            " order by partition_key, id",
            fetch="all",
        )
        ids_by_partition = {}
        for partition_key, id_ in rows:
            ids_by_partition.setdefault(partition_key, []).append(id_)
        return ids_by_partition

    def test_incremental_dynamic_overwrite(self, project):
        assert len(run_dbt(["seed"])) == 2

        # First run creates the Iceberg table.
        assert len(run_dbt(["run", "--vars", "seed_name: partition_1"])) == 1
        assert self._ids_by_partition(project) == {1: [1, 2, 3, 4, 5]}

        # Second run must reach the insert instead of re-creating the table.
        assert len(run_dbt(["run", "--vars", "seed_name: partition_1_replaced_plus_2"])) == 1

        # Partition 1 is replaced in place rather than appended to, and the
        # partition the query newly produced is created.
        assert self._ids_by_partition(project) == {
            1: [6, 7, 8, 9, 10],
            2: [11, 12, 13, 14, 15],
        }

        # Dynamic overwrite leaves a partition the query no longer produces alone.
        assert len(run_dbt(["run", "--vars", "seed_name: partition_1"])) == 1
        assert self._ids_by_partition(project) == {
            1: [1, 2, 3, 4, 5],
            2: [11, 12, 13, 14, 15],
        }

        # External catalogs have no ALTER TABLE ... SWAP WITH, so --full-refresh
        # drops and recreates: only what the query produces survives.
        assert len(run_dbt(["run", "--full-refresh", "--vars", "seed_name: partition_1"])) == 1
        assert self._ids_by_partition(project) == {1: [1, 2, 3, 4, 5]}
