import pytest
from dbt.tests.util import run_dbt

# A nested Iceberg namespace, addressed in StarRocks as one dotted database name.
# StarRocks splits it on `.` when creating it, so this is a real two-level namespace.
NAMESPACE = "dbt_nested_ns.child"

seed_base_csv = """
id,name
1,Alice
2,Bob
3,Charlie
""".lstrip()

nested_table_sql = f"""
{{{{
    config(
        materialized = 'table',
        catalog = 'iceberg_catalog',
        database = '{NAMESPACE}',
    )
}}}}
select * from {{{{ ref('base') }}}}
""".lstrip()

# Reads the nested-namespace table back through source(), so a relation declared
# with a dotted schema is rendered outside the materialization that built it.
# (Not ref(): a model's `catalog`/`database` configs don't reach its ref() relation.)
sources_yml = f"""
version: 2
sources:
  - name: nested
    database: iceberg_catalog
    schema: '{NAMESPACE}'
    tables:
      - name: nested_table
""".lstrip()

downstream_sql = """
{{ config(materialized = 'table') }}
select * from {{ source('nested', 'nested_table') }}
""".lstrip()


class TestNestedNamespaceTable:
    """A table model in a nested Iceberg namespace (#142).

    The namespace used to be split on `.` and truncated, rendering
    `iceberg_catalog`.`dbt_nested_ns`.`child`.`nested_table`, which StarRocks rejects.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seed_base_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nested_table.sql": nested_table_sql,
            "sources.yml": sources_yml,
            "downstream.sql": downstream_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_nested_namespace(self, project):
        project.run_sql(f"""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.`{NAMESPACE}`
            PROPERTIES ("location" = "s3://warehouse/dbt_nested_ns/child")
        """)
        yield

        project.run_sql(f"DROP TABLE IF EXISTS iceberg_catalog.`{NAMESPACE}`.nested_table")
        # The parent namespace is implicit and goes with its last child
        project.run_sql(f"DROP DATABASE IF EXISTS iceberg_catalog.`{NAMESPACE}` FORCE")

    def test_table_in_nested_namespace(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Build the nested table first: dbt doesn't order a model after a source
        results = run_dbt(["run", "--select", "nested_table"])
        assert len(results) == 1

        results = run_dbt(["run", "--select", "downstream"])
        assert len(results) == 1

        nested_count = project.run_sql(
            f"select count(*) from iceberg_catalog.`{NAMESPACE}`.nested_table",
            fetch="one",
        )
        assert nested_count[0] == 3

        downstream_count = project.run_sql(
            f"select count(*) from {project.test_schema}.downstream",
            fetch="one",
        )
        assert downstream_count[0] == 3
