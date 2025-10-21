import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)

base_table_model_with_props_sql = """
{{ 
    config(
        materialized = 'table', 
        engine='OLAP', 
        distributed_by=['id'], 
        buckets="2 ",
        properties={"replication_num":"1", 'colocate_with': 'cg1'}
    ) 
}}
select * from {{ ref('base') }}
""".lstrip()

create_table_with_props_sql = """
CREATE TABLE `table_model_with_props` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(65533) NULL COMMENT "",
  `some_date` datetime NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`id`, `name`)
DISTRIBUTED BY HASH(`id`) BUCKETS 2 
PROPERTIES (
"colocate_with" = "cg1",
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
""".replace("\n", "").replace(" ", "")

# StarRocks doesn't support materialization from table to view https://github.com/StarRocks/dbt-starrocks/issues/33
class TestSimpleMaterializationsMyAdapter(BaseSimpleMaterializations):

    def test_base(self, project):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        if project.test_config.get("require_full_refresh", False):  # required for BigQuery
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable", "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)

class TestSimpleMaterializationWithProperties(BaseSimpleMaterializations):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_with_props.sql": base_table_model_with_props_sql,
        }

    @pytest.mark.skip()
    def test_base(self, project):
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        # run result length
        assert len(results) == 1

        relation = results[0].node.relation_name
        result = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert result.replace(' ', '').replace('\n', '') == create_table_with_props_sql


class TestSingularTestsMyAdapter(BaseSingularTests):
    pass


class TestSingularTestsEphemeralMyAdapter(BaseSingularTestsEphemeral):
    pass


class TestEmptyMyAdapter(BaseEmpty):
    pass


class TestEphemeralMyAdapter(BaseEphemeral):
    pass


class TestIncrementalMyAdapter(BaseIncremental):
    pass


class TestGenericTestsMyAdapter(BaseGenericTests):
    pass

# https://github.com/StarRocks/dbt-starrocks/issues/31
@pytest.mark.skip(reason="outstanding issue")
class TestSnapshotCheckColsMyAdapter(BaseSnapshotCheckCols):
    pass

# https://github.com/StarRocks/dbt-starrocks/issues/29
@pytest.mark.skip(reason="outstanding issue")
class TestSnapshotTimestampMyAdapter(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
