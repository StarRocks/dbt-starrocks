import pytest

from abc import ABC, abstractmethod

from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name

partition_1_base_csv = """
id,partition_key
1,1
2,1
3,1
4,1
5,1
""".lstrip()

partition_1_added_csv = """
id,partition_key
6,1
7,1
8,1
9,1
10,1
""".lstrip()

partition_2_base_csv = """
id,partition_key
11,2
12,2
13,2
14,2
15,2
""".lstrip()

full_partition_1_csv = partition_1_base_csv + """
6,1
7,1
8,1
9,1
10,1
""".lstrip()

full_partition_2_csv = full_partition_1_csv + """
11,2
12,2
13,2
14,2
15,2
""".lstrip()

dynamic_partition_2_csv = partition_1_added_csv + """
11,2
12,2
13,2
14,2
15,2
""".lstrip()

class TestBaseIncrementalStrategyModel(ABC, BaseIncremental):

    @staticmethod
    @abstractmethod
    def _get_strategy():
        raise NotImplementedError

    @abstractmethod
    def _specific_assertions(self, project):
        raise NotImplementedError

    @staticmethod
    def _seeds():
        return {
            "partition_1_base.csv": partition_1_base_csv,
            "partition_1_added.csv": partition_1_added_csv,
            "partition_2_base.csv": partition_2_base_csv,
            "full_partition_1.csv": full_partition_1_csv,
            "full_partition_2.csv": full_partition_2_csv,
            "dynamic_partition_2.csv": dynamic_partition_2_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_incremental",
            "models": {
                "+materialized": "incremental",
                "+partition_type": "Expr",
                "+partition_by": ["partition_key"],
            } | self._get_strategy()
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return self._seeds()

    def _common_assertions(self, project):
        _seeds_row_counts = [
            ("partition_1_base", 5),
            ("partition_1_added", 5),
            ("partition_2_base", 5),
            ("full_partition_1", 10),
            ("full_partition_2", 15),
            ("dynamic_partition_2", 10),
        ]

        # seed command
        results = run_dbt(["seed"])
        assert len(results) == len(_seeds_row_counts)

        # Make sure seeds are properly setup
        for pname, pcount in _seeds_row_counts:
            relation = relation_from_name(project.adapter, f"{pname}")
            result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
            assert result[0] == pcount

        assert len(run_dbt(["run", "--vars", "seed_name: partition_1_base"])) == 1
        check_relations_equal(project.adapter, ["partition_1_base", "incremental"])

        assert len(run_dbt(["run", "--vars", "seed_name: partition_1_added"])) == 1

    def _doc_tests(self):
        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == len(self._seeds()) + 1

    def test_incremental(self, project):
        self._common_assertions(project=project)
        self._specific_assertions(project=project)
        self._doc_tests()


class TestInvalidStrategyIncrementalModel(TestBaseIncrementalStrategyModel):


    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "foobar"}

    def _specific_assertions(self, project):
        pass

    def test_incremental(self, project):
        try:
            run_dbt(["run"])
        except AssertionError as ae:
            assert str(ae) == "dbt exit state did not match expected"


class TestOmittedStrategyIncrementalModel(TestBaseIncrementalStrategyModel):

    @staticmethod
    def _get_strategy():
        return {}

    def _specific_assertions(self, project):
        check_relations_equal(project.adapter, ["full_partition_1", "incremental"])

        assert len(run_dbt(["run", "--vars", "seed_name: partition_2_base"])) == 1
        check_relations_equal(project.adapter, ["full_partition_2", "incremental"])


class TestDefaultStrategyIncrementalModel(TestOmittedStrategyIncrementalModel):

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "default"}


class TestInsertOverwriteStrategyIncrementalModel(TestBaseIncrementalStrategyModel):

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "insert_overwrite"}

    def _specific_assertions(self, project):
        check_relations_equal(project.adapter, ["partition_1_added", "incremental"])

        assert len(run_dbt(["run", "--vars", "seed_name: partition_2_base"])) == 1
        check_relations_equal(project.adapter, ["partition_2_base", "incremental"])


class TestDynamicOverwriteStrategyIncrementalModel(TestBaseIncrementalStrategyModel):

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "dynamic_overwrite"}

    def _specific_assertions(self, project):
        check_relations_equal(project.adapter, ["partition_1_added", "incremental"])

        results = run_dbt(["run", "--vars", "seed_name: partition_2_base"])
        assert len(results) == 1

        check_relations_equal(project.adapter, ["dynamic_partition_2", "incremental"])
