import pytest
from dbt.tests.util import run_dbt

model_pk_constraint_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

pk_constraint_schema_yml = """
version: 2
models:
  - name: model_pk_constraint
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: primary_key
      - name: name
        data_type: varchar(255)
""".lstrip()

model_unique_constraint_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

unique_constraint_schema_yml = """
version: 2
models:
  - name: model_unique_constraint
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: unique
      - name: name
        data_type: varchar(255)
""".lstrip()

model_model_level_pk_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

model_level_pk_schema_yml = """
version: 2
models:
  - name: model_model_level_pk
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        data_type: int
      - name: name
        data_type: varchar(255)
""".lstrip()

model_explicit_config_sql = """
{{ config(
    materialized='table',
    table_type='DUPLICATE',
    keys=['id'],
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

explicit_config_schema_yml = """
version: 2
models:
  - name: model_explicit_config
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: primary_key
      - name: name
        data_type: varchar(255)
""".lstrip()


model_not_null_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

not_null_schema_yml = """
version: 2
models:
  - name: model_not_null
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: name
        data_type: varchar(255)
""".lstrip()

model_pk_implicit_not_null_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

pk_implicit_not_null_schema_yml = """
version: 2
models:
  - name: model_pk_implicit_not_null
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: primary_key
      - name: name
        data_type: varchar(255)
""".lstrip()

model_pk_and_not_null_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select 1 as id, 'hello' as name
""".lstrip()

pk_and_not_null_schema_yml = """
version: 2
models:
  - name: model_pk_and_not_null
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: primary_key
          - type: not_null
      - name: name
        data_type: varchar(255)
""".lstrip()


class TestNotNullConstraint:
    """not_null constraint emits NOT NULL inline column DDL."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_not_null.sql": model_not_null_sql,
            "schema.yml": not_null_schema_yml,
        }

    def test_not_null_in_ddl(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "NOT NULL" in ddl


class TestColumnLevelPrimaryKey:
    """Column-level primary_key constraint derives PRIMARY KEY table type."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_pk_constraint.sql": model_pk_constraint_sql,
            "schema.yml": pk_constraint_schema_yml,
        }

    def test_primary_key_table_type(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "PRIMARY KEY" in ddl


class TestColumnLevelUniqueKey:
    """Column-level unique constraint is not supported — dbt warns and creates a DUPLICATE KEY table."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_unique_constraint.sql": model_unique_constraint_sql,
            "schema.yml": unique_constraint_schema_yml,
        }

    def test_unique_constraint_falls_back_to_duplicate(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "DUPLICATE KEY" in ddl
        assert "UNIQUE KEY" not in ddl


class TestModelLevelPrimaryKey:
    """Model-level primary_key constraint derives PRIMARY KEY table type."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_model_level_pk.sql": model_model_level_pk_sql,
            "schema.yml": model_level_pk_schema_yml,
        }

    def test_model_level_pk(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "PRIMARY KEY" in ddl


class TestExplicitConfigOverridesConstraints:
    """Explicit table_type/keys config takes priority over constraints."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_explicit_config.sql": model_explicit_config_sql,
            "schema.yml": explicit_config_schema_yml,
        }

    def test_explicit_config_wins(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "DUPLICATE KEY" in ddl
        assert "PRIMARY KEY" not in ddl


class TestPrimaryKeyImplicitNotNull:
    """PRIMARY KEY columns are implicitly NOT NULL in StarRocks even without an explicit not_null constraint."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_pk_implicit_not_null.sql": model_pk_implicit_not_null_sql,
            "schema.yml": pk_implicit_not_null_schema_yml,
        }

    def test_pk_column_is_not_null(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "PRIMARY KEY" in ddl
        # StarRocks implicitly enforces NOT NULL on all PRIMARY KEY columns
        assert "`id`" in ddl and "NOT NULL" in ddl


class TestPrimaryKeyAndNotNullCombined:
    """Combining primary_key and not_null on the same column is redundant but accepted by StarRocks."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_pk_and_not_null.sql": model_pk_and_not_null_sql,
            "schema.yml": pk_and_not_null_schema_yml,
        }

    def test_pk_and_not_null_succeeds(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = results[0].node.relation_name
        ddl = project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]
        assert "PRIMARY KEY" in ddl
        # Redundant NOT NULL on a PK column is accepted without error
        assert "`id`" in ddl and "NOT NULL" in ddl
