import pytest

from dbt.tests.util import run_dbt

# `properties: {}` is how a project clears a `+properties` value inherited
# from a parent key in dbt_project.yml. It must not render `PROPERTIES ()`.

base_sql = """
{{ config(materialized='table', distributed_by=['id']) }}
select 1 as id
""".lstrip()

table_sql = """
{{ config(materialized='table', distributed_by=['id'], properties={}) }}
select id from {{ ref('base') }}
""".lstrip()

mv_sql = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual',
    properties={}
) }}
select id from {{ ref('base') }}
""".lstrip()


class TestEmptyProperties:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base.sql": base_sql,
            "empty_props_table.sql": table_sql,
            "empty_props_mv.sql": mv_sql,
        }

    def test_empty_properties_succeeds(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
