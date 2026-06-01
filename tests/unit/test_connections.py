from mysql.connector.constants import FieldType

from dbt.adapters.starrocks.connections import StarRocksConnectionManager


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
