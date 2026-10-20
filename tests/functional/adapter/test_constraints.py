import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

# Seed columns are created nullable, so NOT NULL in a model's DDL can only come
# from the table definition, not from CTAS type inference on constants.
source_seed_csv = """
id,name
1,a
2,b
""".lstrip()

default_model_sql = """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select id, name from {{ ref('source_seed') }}
""".lstrip()


def schema_yml(model_name, id_constraints="", name_constraints="", model_constraints="",
               id_type="bigint", extra_columns=""):
    return f"""
version: 2
models:
  - name: {model_name}
    config:
      contract:
        enforced: true
{model_constraints}
    columns:
      - name: id
        data_type: {id_type}
{id_constraints}
      - name: name
        data_type: varchar(255)
{name_constraints}
{extra_columns}
""".lstrip()


pk_column_constraint = """
        constraints:
          - type: primary_key
""".strip("\n")

pk_model_constraint = """
    constraints:
      - type: primary_key
        columns: [id]
""".strip("\n")

not_null_column_constraint = """
        constraints:
          - type: not_null
""".strip("\n")

unique_column_constraint = """
        constraints:
          - type: unique
""".strip("\n")


def show_create_table(project, relation):
    return project.run_sql(f"SHOW CREATE TABLE {relation}", fetch="one")[1]


def column_line(ddl, column):
    """Return the DDL line defining `column`, so assertions target that column only."""
    lines = [line for line in ddl.splitlines() if line.strip().startswith(f"`{column}`")]
    assert len(lines) == 1, f"expected one definition of `{column}` in:\n{ddl}"
    return lines[0]


class ConstraintTestBase:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source_seed.csv": source_seed_csv}

    def build(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        return show_create_table(project, results[0].node.relation_name)


class TestColumnLevelPrimaryKey(ConstraintTestBase):
    """Column-level primary_key constraint derives a PRIMARY KEY table; only the key is NOT NULL."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_pk_constraint.sql": default_model_sql,
            "schema.yml": schema_yml("model_pk_constraint", id_constraints=pk_column_constraint),
        }

    def test_primary_key_table_type(self, project):
        ddl = self.build(project)
        assert "PRIMARY KEY(`id`)" in ddl
        # StarRocks makes PRIMARY KEY columns NOT NULL; other columns keep the source's nullability
        assert "NOT NULL" in column_line(ddl, "id")
        assert "NOT NULL" not in column_line(ddl, "name")


class TestModelLevelPrimaryKey(ConstraintTestBase):
    """Model-level primary_key constraint derives a PRIMARY KEY table."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_model_level_pk.sql": default_model_sql,
            "schema.yml": schema_yml("model_model_level_pk", model_constraints=pk_model_constraint),
        }

    def test_model_level_pk(self, project):
        ddl = self.build(project)
        assert "PRIMARY KEY(`id`)" in ddl


class TestColumnLevelUniqueKey(ConstraintTestBase):
    """Column-level unique constraint is not supported and does not change the table type."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_unique_constraint.sql": default_model_sql,
            "schema.yml": schema_yml("model_unique_constraint", id_constraints=unique_column_constraint),
        }

    def test_unique_constraint_falls_back_to_duplicate(self, project):
        ddl = self.build(project)
        assert "DUPLICATE KEY" in ddl
        assert "UNIQUE KEY" not in ddl
        assert "PRIMARY KEY" not in ddl


class TestNotNullNotApplied(ConstraintTestBase):
    """not_null is not supported: CTAS cannot declare NOT NULL, so the column stays nullable."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_not_null.sql": default_model_sql,
            "schema.yml": schema_yml("model_not_null", name_constraints=not_null_column_constraint),
        }

    def test_not_null_is_not_emitted(self, project):
        ddl = self.build(project)
        assert "NOT NULL" not in column_line(ddl, "name")


class TestExplicitConfigOverridesConstraints(ConstraintTestBase):
    """Explicit table_type/keys config takes priority over constraints."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_explicit_config.sql": """
{{ config(
    materialized='table',
    table_type='DUPLICATE',
    keys=['id'],
    distributed_by=['id'],
) }}
select id, name from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml("model_explicit_config", id_constraints=pk_column_constraint),
        }

    def test_explicit_config_wins(self, project):
        ddl = self.build(project)
        assert "DUPLICATE KEY(`id`)" in ddl
        assert "PRIMARY KEY" not in ddl


class TestExplicitTableTypeOverridesConstraints(ConstraintTestBase):
    """Explicit table_type without keys still takes priority over a primary_key constraint."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_explicit_table_type.sql": """
{{ config(
    materialized='table',
    table_type='DUPLICATE',
    distributed_by=['id'],
) }}
select id, name from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml("model_explicit_table_type", id_constraints=pk_column_constraint),
        }

    def test_explicit_table_type_wins(self, project):
        ddl = self.build(project)
        assert "DUPLICATE KEY" in ddl
        assert "PRIMARY KEY" not in ddl


class TestContractTypeMismatchFails(ConstraintTestBase):
    """An enforced contract fails the build when a column's type differs from the YAML."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_type_mismatch.sql": default_model_sql,
            "schema.yml": schema_yml("model_type_mismatch", id_type="varchar(255)"),
        }

    def test_type_mismatch_fails(self, project):
        run_dbt(["seed"])
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "contract" in log.lower()
        assert "data type mismatch" in log.lower()


class TestContractMissingColumnFails(ConstraintTestBase):
    """An enforced contract fails the build when the query lacks a declared column."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_missing_column.sql": default_model_sql,
            "schema.yml": schema_yml(
                "model_missing_column",
                extra_columns="      - name: created_at\n        data_type: datetime",
            ),
        }

    def test_missing_column_fails(self, project):
        run_dbt(["seed"])
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "contract" in log.lower()
        assert "created_at" in log


class TestPrimaryKeyMustBeSelectedFirst(ConstraintTestBase):
    """StarRocks requires PRIMARY KEY columns to lead the schema, so the SELECT order matters."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_pk_not_first.sql": """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select name, id from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml("model_pk_not_first", id_constraints=pk_column_constraint),
        }

    def test_pk_not_first_fails(self, project):
        run_dbt(["seed"])
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "Key columns must be the first few columns" in log


class TestExplicitPrimaryTableTypeDerivesKeys(ConstraintTestBase):
    """Explicit table_type='primary' without keys derives the keys from the primary_key constraint."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_explicit_primary.sql": """
{{ config(
    materialized='table',
    table_type='primary',
    distributed_by=['id'],
) }}
select id, name from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml("model_explicit_primary", id_constraints=pk_column_constraint),
        }

    def test_explicit_primary_uses_constraint_keys(self, project):
        ddl = self.build(project)
        assert "PRIMARY KEY(`id`)" in ddl


class TestContractFloatBecomesDecimal(ConstraintTestBase):
    """The contract checks the query's types, but CTAS stores FLOAT/DOUBLE as DECIMAL(38, 9)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_double.sql": """
{{ config(
    materialized='table',
    distributed_by=['id'],
) }}
select id, name, cast(id as double) as score from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml(
                "model_double",
                extra_columns="      - name: score\n        data_type: double",
            ),
        }

    def test_double_contract_passes_but_column_is_decimal(self, project):
        ddl = self.build(project)
        assert "decimal(38, 9)" in column_line(ddl, "score")


class TestConstraintWarnings(ConstraintTestBase):
    """Unsupported and unenforced constraints emit dbt's standard warnings."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_warnings.sql": default_model_sql,
            "schema.yml": schema_yml(
                "model_warnings",
                id_constraints=pk_column_constraint,
                name_constraints=not_null_column_constraint,
            ),
        }

    def test_warnings(self, project):
        run_dbt(["seed"])
        _, log = run_dbt_and_capture(["run"])
        log = " ".join(log.split())  # dbt wraps warnings across lines
        assert "constraint type not_null is not supported by starrocks" in log
        assert "constraint type primary_key is not enforced by starrocks" in log


class TestViewContractMismatchFails(ConstraintTestBase):
    """Views check enforced contracts too."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_view_mismatch.sql": """
{{ config(materialized='view') }}
select id, name from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml("model_view_mismatch", id_type="varchar(255)"),
        }

    def test_view_type_mismatch_fails(self, project):
        run_dbt(["seed"])
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "data type mismatch" in log.lower()


class TestViewContractPasses(ConstraintTestBase):
    """A view whose query matches its contract builds normally."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_view_ok.sql": """
{{ config(materialized='view') }}
select id, name from {{ ref('source_seed') }}
""".lstrip(),
            "schema.yml": schema_yml("model_view_ok"),
        }

    def test_view_builds(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["run"])) == 1
