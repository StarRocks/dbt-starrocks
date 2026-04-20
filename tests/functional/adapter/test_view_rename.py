import pytest
from dbt.tests.util import run_dbt


simple_view_sql = """
{{ config(materialized='view') }}
select 1 as id, 'hello' as name
"""

dependent_view_sql = """
{{ config(materialized='view') }}
select id, name from {{ ref('base_view') }}
"""


class TestViewFullRefreshSkipsBackup:
    """Test that full-refresh on views works without stale definition errors.

    When renaming a view to __dbt_backup, the adapter should simply drop
    the original view instead of trying to recreate it from information_schema,
    which can fail when upstream views have already been rebuilt.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_view.sql": simple_view_sql,
            "dependent_view.sql": dependent_view_sql,
        }

    def test_full_refresh_views(self, project):
        # Initial run creates both views
        results = run_dbt(["run"])
        assert len(results) == 2

        # Full refresh triggers rename to __dbt_backup then recreate.
        # This must succeed even though the backup rename skips recreation.
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 2
