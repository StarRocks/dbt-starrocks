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


def _server_version(project) -> tuple:
    """Return the running StarRocks version as a (major, minor, patch) tuple."""
    raw = project.run_sql("select current_version()", fetch="one")[0]
    first = raw.split('-')[0].split(' ')[0]
    parts = first.split('.')
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        return tuple(int(p) for p in parts)
    return (999, 999, 999)


def _skip_if_before(project, version: tuple, reason: str) -> None:
    """Skip the current test when the server is older than `version`.

    Verbatim view definition storage — which the skip comparison depends on —
    only exists from 4.0.6 (views, #68040). Below that StarRocks canonicalizes
    the stored SQL, so the skip optimization is intentionally disabled and these
    assertions don't hold.
    """
    if _server_version(project) < version:
        pytest.skip(reason)


class TestViewSkipWhenUnchanged:
    """
    Tests the StarRocks view materialization's skip-when-unchanged behavior.

    On >= 4.0.6 StarRocks stores the original view SQL verbatim, so the
    materialization compares the stored definition against the compiled SQL and
    issues no DDL when they match. Because StarRocks deactivates dependent MVs
    whenever a base view is recreated, skipping the unchanged view keeps those
    MVs active.

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
        _skip_if_before(project, (4, 0, 6),
                        "view skip requires verbatim view storage (>= 4.0.6)")

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
        _skip_if_before(project, (4, 0, 6),
                        "view skip requires verbatim view storage (>= 4.0.6)")

        run_dbt(["run", "--select", "my_view"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "dependent MV must stay active when the unchanged view is skipped"
        )

    def test_sql_change_rebuilds_view(self, project, my_view):
        """
        A genuine SQL change must rebuild the view (not skip it). Rebuilding the
        view deactivates the dependent MV — confirming the skip path was not taken.
        """
        _skip_if_before(project, (4, 0, 6),
                        "view skip requires verbatim view storage (>= 4.0.6)")

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
        assert "id >= 1" in stored, "rebuilt view should reflect the new SQL"

        # Rebuilding the base view deactivates the dependent MV.
        assert _is_active(project, "my_mv_on_view") == "false", (
            "rebuilding the base view should deactivate the dependent MV"
        )
