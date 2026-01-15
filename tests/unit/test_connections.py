import pytest
from unittest.mock import Mock, patch
from dbt.adapters.starrocks.connections import StarRocksConnectionManager


class TestStarRocksTransactionHandling:
    """
    Test transaction handling in StarRocks adapter.

    StarRocks does not support DDL statements (CREATE, DROP, ALTER, etc.)
    in explicit transactions.

    Error 5305 (25P01): Explicit transaction only support
    begin/commit/rollback/insert/update/delete/set/select statements.

    Requirements:
    1. DDL statements should NOT be wrapped in transactions (no BEGIN/COMMIT)
    2. DML statements should maintain EXISTING behavior (unchanged)
    """

    # ========== DDL Detection Tests ==========

    @pytest.mark.parametrize(
        "sql,expected",
        [
            # DDL statements - should return True
            ("CREATE TABLE my_table (id INT)", True),
            ("create table my_table (id int)", True),
            ("  CREATE   TABLE  my_table (id INT)", True),
            ("DROP TABLE my_table", True),
            ("drop table if exists my_table", True),
            ("ALTER TABLE my_table ADD COLUMN name STRING", True),
            ("CREATE VIEW my_view AS SELECT * FROM t", True),
            ("DROP VIEW my_view", True),
            ("CREATE DATABASE my_db", True),
            ("DROP DATABASE my_db", True),
            ("TRUNCATE TABLE my_table", True),
            # DML statements - should return False
            ("INSERT INTO my_table VALUES (1, 'a')", False),
            ("insert into my_table values (1, 'a')", False),
            ("UPDATE my_table SET name = 'b' WHERE id = 1", False),
            ("DELETE FROM my_table WHERE id = 1", False),
            ("SELECT * FROM my_table", False),
            ("select * from my_table where id = 1", False),
            # Other statements - should return False
            ("SET session_variable = 'value'", False),
            ("BEGIN", False),
            ("COMMIT", False),
            ("ROLLBACK", False),
        ],
    )
    def test_is_ddl_statement_detection(self, sql, expected):
        """Test that DDL statements are correctly identified."""
        result = StarRocksConnectionManager._is_ddl_statement(sql)
        assert result == expected

    # ========== DDL Flow Test ==========

    def test_ddl_execution_no_transaction(self):
        """
        DDL execution should NOT have BEGIN/COMMIT.

        Flow:
        1. add_begin_query() -> Empty (no BEGIN)
        2. add_query("CREATE TABLE ...") -> Executes DDL
        3. add_commit_query() -> Empty (no COMMIT)
        """
        queries_executed = []

        def track_query(sql, *args, **kwargs):
            if sql.strip():
                queries_executed.append(sql)
            return (Mock(), Mock())

        with patch.object(
            StarRocksConnectionManager, "add_query", side_effect=track_query
        ):
            manager = object.__new__(StarRocksConnectionManager)
            manager._is_ddl_context = True
            ddl_sql = "CREATE TABLE my_table (id INT)"

            manager.add_begin_query()
            manager.add_query(ddl_sql)
            manager.add_commit_query()

            assert ddl_sql in queries_executed, "DDL should be executed"
            assert "BEGIN" not in queries_executed, "No BEGIN for DDL"
            assert "COMMIT" not in queries_executed, "No COMMIT for DDL"

    # ========== DML Flow Test ==========

    def test_dml_execution_with_commit(self):
        """
        DML execution should have COMMIT (but no BEGIN).

        Flow:
        1. add_begin_query() -> Empty (no BEGIN)
        2. add_query("INSERT INTO ...") -> Executes DML
        3. add_commit_query() -> COMMIT
        """
        queries_executed = []

        def track_query(sql, *args, **kwargs):
            queries_executed.append(sql)
            return (Mock(), Mock())

        with patch.object(
            StarRocksConnectionManager, "add_query", side_effect=track_query
        ):
            manager = object.__new__(StarRocksConnectionManager)
            manager._is_ddl_context = False
            dml_sql = "INSERT INTO my_table VALUES (1, 'test')"

            manager.add_begin_query()
            manager.add_query(dml_sql)
            manager.add_commit_query()

            assert dml_sql in queries_executed, "DML should be executed"
            assert "BEGIN" not in queries_executed, "No BEGIN (existing behavior)"
            assert "COMMIT" in queries_executed, "COMMIT should be sent for DML"
