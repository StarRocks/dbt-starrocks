import pytest

from dbt.tests.util import (
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from dbt.adapters.contracts.relation import RelationType

MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()

# A view and a materialized view built on top of it. The MV is only used to
# prove that skipping an unchanged view keeps its dependent MV active; the MV
# itself is never re-run in these tests.

MY_VIEW_SQL = """
{{ config(materialized='view') }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

# A genuine SQL change that does NOT alter column types, so the dependent
# passthrough MV stays schema-compatible. (Changing a column's type, e.g.
# value * 10, would make StarRocks reject reactivation.)
MY_VIEW_SQL_CHANGED = """
{{ config(materialized='view') }}
select id, value from {{ ref('my_seed') }} where id >= 1
""".lstrip()

MY_MV_ON_MY_VIEW_SQL = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual'
) }}
select id, value from {{ ref('my_view') }}
""".lstrip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_active(project, mv_name: str) -> str:
    """Return IS_ACTIVE string ('true' or 'false') for the named MV."""
    schema = project.test_schema
    result = project.run_sql(
        f"select is_active from information_schema.materialized_views"
        f" where table_schema = '{schema}' and table_name = '{mv_name}'",
        fetch="one",
    )
    assert result is not None, f"MV '{mv_name}' not found in information_schema"
    return result[0]


class TestViewSkipWhenUnchanged:
    """
    Tests the StarRocks view materialization's skip-when-unchanged behavior.

    The materialization builds the candidate under a temporary name, lets the
    server canonicalize it, and compares its stored definition against the
    existing view's. Because both strings come from the same engine, this works
    on every StarRocks version (verbatim on >= 4.0.6, re-qualified on 3.5) and
    issues no DDL on the target when they match. Because StarRocks deactivates
    dependent MVs whenever a base view is recreated, skipping the unchanged view
    keeps those MVs active.

    Setup: my_seed -> my_view (view) -> my_mv_on_view (materialized_view).
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_view.sql": MY_VIEW_SQL,
            "my_mv_on_view.sql": MY_MV_ON_MY_VIEW_SQL,
        }

    @pytest.fixture(scope="class")
    def my_view(self, project):
        return project.adapter.Relation.create(
            identifier="my_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.View,
        )

    @pytest.fixture(autouse=True)
    def setup(self, project, my_view):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])
        initial_model = get_model_file(project, my_view)
        yield
        set_model_file(project, my_view, initial_model)
        project.run_sql(f"drop database if exists {project.test_schema} force")

    def test_unchanged_view_is_skipped(self, project, my_view):
        """An unchanged view must issue no DDL — logged as 'skip <relation>'."""
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_view"])

        assert f"skip {my_view}" in logs, (
            "an unchanged view should be skipped (no DDL issued)"
        )

    def test_unchanged_view_run_keeps_dependent_mv_active(self, project):
        """
        Running ONLY the (unchanged) view must not deactivate its dependent MV.
        Because the MV model is not run here, the MV staying active proves the
        view issued no DDL.
        """
        run_dbt(["run", "--select", "my_view"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "dependent MV must stay active when the unchanged view is skipped"
        )

    def test_sql_change_rebuilds_view(self, project, my_view):
        """
        A genuine SQL change must rebuild the view (not skip it). Rebuilding the
        view deactivates the dependent MV — confirming the skip path was not taken.
        """
        set_model_file(project, my_view, MY_VIEW_SQL_CHANGED)

        # Rebuild just the view: the SQL changed, so it must not be skipped.
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_view"])
        assert f"skip {my_view}" not in logs, (
            "a changed view must be rebuilt, not skipped"
        )
        stored = project.run_sql(
            f"select view_definition from information_schema.views"
            f" where table_schema = '{project.test_schema}' and table_name = 'my_view'",
            fetch="one",
        )[0]
        # Version-agnostic: 3.5 stores the predicate backtick-qualified
        # (`my_seed`.`id` >= 1), 4.0.6+ stores it verbatim (id >= 1).
        assert ">= 1" in stored, "rebuilt view should reflect the new SQL predicate"

        # Rebuilding the base view deactivates the dependent MV.
        assert _is_active(project, "my_mv_on_view") == "false", (
            "rebuilding the base view should deactivate the dependent MV"
        )
