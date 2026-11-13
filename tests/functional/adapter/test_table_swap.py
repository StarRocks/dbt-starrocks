import pytest

from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture

swap_sql = """
{{ config(materialized='table', on_table_exists='swap', distributed_by=['id']) }}
{% if var('broken', false) %}
select id from {{ this.schema }}.table_that_does_not_exist
{% else %}
select id from (select 1 as id union all select 2 as id) t
where id <= {{ var('max_id', 1) }}
{% endif %}
""".lstrip()


class TestTableOnTableExistsSwap:

    @pytest.fixture(scope="class")
    def models(self):
        return {"swapped.sql": swap_sql}

    def _row_count(self, project):
        relation = relation_from_name(project.adapter, "swapped")
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    def _table_names(self, project):
        rows = project.run_sql(
            "select table_name from information_schema.tables where table_schema = '{schema}'",
            fetch="all",
        )
        return sorted(row[0] for row in rows)

    def test_swap(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "swap with" not in logs.lower()
        assert self._row_count(project) == 1

        _, logs = run_dbt_and_capture(["--debug", "run", "--vars", "max_id: 2"])
        assert "swap with" in logs.lower()
        drops = [line for line in logs.lower().splitlines() if "drop table if exists" in line]
        assert drops and all("__dbt_tmp" in line for line in drops)
        assert self._row_count(project) == 2
        assert self._table_names(project) == ["swapped"]

        run_dbt(["run", "--full-refresh", "--vars", "max_id: 1"])
        assert self._row_count(project) == 1
        assert self._table_names(project) == ["swapped"]

        run_dbt(["run", "--vars", "broken: true"], expect_pass=False)
        assert self._row_count(project) == 1

        run_dbt(["run", "--vars", "max_id: 2"])
        assert self._row_count(project) == 2
        assert self._table_names(project) == ["swapped"]
