import pytest

from dbt.tests.util import relation_from_name, run_dbt

# A seed with a decimal column reproduces the original bug: the base adapter
# emitted "float8" for `rate`, which StarRocks fails to parse.
decimal_seed_csv = """
id,rate
a,0.06
b,0.04
c,0.0625
""".lstrip()


class TestSeedWithDecimalColumn:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"decimal_seed.csv": decimal_seed_csv}

    def test_seed_with_decimal(self, project):
        # seed command must succeed (used to fail with a syntax error on float8)
        results = run_dbt(["seed"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "decimal_seed")

        # all rows landed
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 3

        # the decimal column was created as a StarRocks "double", not "float8"
        col_type = project.run_sql(
            f"select data_type from information_schema.columns "
            f"where table_schema = '{relation.schema}' "
            f"and table_name = '{relation.identifier}' "
            f"and column_name = 'rate'",
            fetch="one",
        )
        assert col_type[0].lower() == "double"
