"""
Unit tests for connection helpers in dbt.adapters.starrocks.connections.
"""
import pytest
from mysql.connector.constants import FieldType

from dbt.adapters.starrocks.connections import StarRocksConnectionManager, _parse_version



def test_data_type_code_to_name_maps_mysql_connector_type_codes():
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.DECIMAL) == "decimal"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.DATE) == "date"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.DATETIME) == "datetime"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.TIMESTAMP) == "datetime"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.LONGLONG) == "bigint"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.NEWDECIMAL) == "decimal"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.TIME) == "varchar"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.YEAR) == "smallint"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.VAR_STRING) == "varchar"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.BLOB) == "varbinary"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.BIT) == "tinyint"


def test_data_type_code_to_name_normalizes_string_type_codes():
    assert StarRocksConnectionManager.data_type_code_to_name("VARCHAR") == "varchar"


def test_data_type_code_to_name_falls_back_to_mysql_connector_name():
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.GEOMETRY) == "geometry"


def test_data_type_code_to_name_logs_unknown_numeric_code():
    assert StarRocksConnectionManager.data_type_code_to_name(99999) == "99999"


DEFAULT = (999, 999, 999)


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Standard "<version>-<commit>" form returned by select current_version().
        ("4.0.7-b75f536", (4, 0, 7)),
        ("4.0.6-abc", (4, 0, 6)),
        ("4.0.5-x", (4, 0, 5)),
        ("4.1.0-rc01", (4, 1, 0)),
        ("3.5.14-x", (3, 5, 14)),
        ("4.0.10-x", (4, 0, 10)),
        ("3.5.17-deadbeef", (3, 5, 17)),
        ("4.0.7", (4, 0, 7)),
        ("8.0.33 (StarRocks)", (8, 0, 33)),
        # Anything that is not exactly three numeric parts falls back to the
        # sentinel default.
        ("4.0", DEFAULT),
        ("3.5", DEFAULT),
        ("garbage", DEFAULT),
        ("4", DEFAULT),
        ("4.0.x", DEFAULT),
        ("", DEFAULT),
    ],
)
def test_parse_version(raw, expected):
    assert _parse_version(raw) == expected
