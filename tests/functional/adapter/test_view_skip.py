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

# Same SQL as MY_VIEW_SQL but materialized as a table — used to seed a
# wrong-type relation at the model's name before switching it to a view.
MY_VIEW_AS_TABLE_SQL = """
{{ config(materialized='table', distributed_by=['id']) }}
select id, value from {{ ref('my_seed') }}
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

    def test_full_refresh_rebuilds_unchanged_view(self, project, my_view):
        """
        `--full-refresh` must rebuild even an unchanged view (the usual escape
        hatch), so the skip optimization is bypassed and the dependent MV is
        deactivated — matching the table/incremental materializations.
        """
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--full-refresh", "--select", "my_view"]
        )
        assert f"skip {my_view}" not in logs, (
            "--full-refresh must force a rebuild, not skip"
        )
        assert _is_active(project, "my_mv_on_view") == "false", (
            "a forced rebuild should deactivate the dependent MV"
        )

    def test_skip_check_uses_info_schema_for_internal_catalog(self, project, my_view):
        """
        The stored-view-definition probe dispatches per catalog kind: internal
        catalogs read from `information_schema.views`, external catalogs use
        `SHOW CREATE VIEW`. For an internal-catalog view the dispatcher must
        take the info_schema path and the external-only `SHOW CREATE VIEW`
        path must NOT be taken.
        """
        # This test pins the dispatcher's internal-catalog branch; if the
        # profile is ever pointed at a non-default catalog the assumption
        # changes (and this test would need to assert the opposite).
        assert project.adapter.config.credentials.catalog == "default_catalog", (
            "this test assumes the internal catalog so the dispatcher should "
            "pick the info_schema probe; reconfigure if profile changes"
        )

        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_view"])

        assert "information_schema.views" in logs, (
            "internal-catalog dispatch should probe information_schema.views; "
            "not found in run logs"
        )
        assert "show create view" not in logs.lower(), (
            "internal-catalog dispatch should not invoke SHOW CREATE VIEW; "
            "found the external-branch query in the run logs"
        )


class TestViewReplacesWrongTypeRelation:
    """
    When a model switches from `materialized='table'` to `materialized='view'`,
    the existing table is renamed to a `__dbt_backup` so the original survives
    a failed `create view`; on success the backup is dropped at the end. This
    test exercises the rename + drop-on-success path and asserts no backup
    table is left behind.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_view.sql": MY_VIEW_AS_TABLE_SQL}

    @pytest.fixture(scope="class")
    def my_view(self, project):
        # The model file path is derived from the identifier; the type only
        # matters for the rename logic at run time, not for set_model_file.
        return project.adapter.Relation.create(
            identifier="my_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.Table,
        )

    @pytest.fixture(autouse=True)
    def setup(self, project, my_view):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])  # creates my_view as a table
        yield
        project.run_sql(f"drop database if exists {project.test_schema} force")

    def test_table_is_replaced_by_view_with_no_backup_leftover(self, project, my_view):
        schema = project.test_schema

        # Sanity: initial relation is a table.
        initial = project.run_sql(
            f"select table_type from information_schema.tables"
            f" where table_schema = '{schema}' and table_name = 'my_view'",
            fetch="one",
        )
        assert initial is not None, "expected a table at my_view before the switch"
        assert "VIEW" not in initial[0].upper(), (
            f"my_view should start as a table; got {initial[0]}"
        )

        # Switch the model to a view and re-run.
        set_model_file(project, my_view, MY_VIEW_SQL)
        run_dbt(["run", "--select", "my_view"])

        # The relation at my_view is now a view.
        after = project.run_sql(
            f"select table_type from information_schema.tables"
            f" where table_schema = '{schema}' and table_name = 'my_view'",
            fetch="one",
        )
        assert after is not None, "expected a view at my_view after the switch"
        assert after[0].upper() == "VIEW", (
            f"my_view should now be a view; got {after[0]}"
        )

        # The __dbt_backup table was dropped after the successful create.
        # (StarRocks doesn't support LIKE ... ESCAPE, so filter in Python.)
        rows = project.run_sql(
            f"select table_name from information_schema.tables"
            f" where table_schema = '{schema}'",
            fetch="all",
        )
        leftover = [r[0] for r in rows if r[0].endswith("__dbt_backup")]
        assert leftover == [], (
            f"no __dbt_backup relations should remain after success; found {leftover}"
        )
