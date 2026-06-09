import agate

from dbt.adapters.starrocks.impl import StarRocksAdapter


def _table(values):
    """Build a single-column agate table inferred from *values*."""
    return agate.Table.from_object([{"col": v} for v in values])


class TestConvertNumberType:
    """Regression tests for `convert_number_type`.

    The base SQLAdapter implementation returns ``"float8"`` for any numeric
    column with decimals, which is a Postgres alias StarRocks doesn't parse.
    """

    def test_decimal_column_maps_to_double(self):
        table = _table(["0.06", "0.04", "0.0625"])
        assert StarRocksAdapter.convert_number_type(table, 0) == "double"

    def test_integer_column_maps_to_bigint(self):
        table = _table(["1", "2", "3"])
        assert StarRocksAdapter.convert_number_type(table, 0) == "bigint"

    def test_mixed_int_and_decimal_treated_as_decimal(self):
        table = _table(["1", "2.5", "3"])
        assert StarRocksAdapter.convert_number_type(table, 0) == "double"
