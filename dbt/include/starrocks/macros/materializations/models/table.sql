/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https:*www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{% macro starrocks__create_table_as(temporary, relation, sql, is_external=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set engine = config.get('engine', 'OLAP') -%}
  {%- set indexs = config.get('indexs') -%}
  {%- set properties = config.get('properties') -%}

  {#- Fails the build if the query's columns or types differ from an enforced contract -#}
  {%- set contract_config = config.get('contract') -%}
  {%- set contract_enforced = contract_config and contract_config.enforced -%}
  {%- if contract_enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif -%}

  {#- An enforced contract on an OLAP table creates the table from the YAML schema, then
      inserts into it, so the declared types and column constraints apply. CTAS cannot
      declare column types or NOT NULL. This includes the incremental staging table, so its
      schema matches the target's for on_schema_change. -#}
  {%- if contract_enforced and not is_external and engine == 'OLAP' -%}
    {%- set create_sql -%}
      {{ sql_header if sql_header is not none }}
      {{ starrocks__create_table_from_contract(relation) }}
    {%- endset -%}
    {%- do run_query(create_sql) -%}

    {{ sql_header if sql_header is not none }}
    insert into {{ relation.include(database=False) }} ({{ starrocks__contract_column_names() }})
    select {{ starrocks__contract_column_names() }}
    from (
      {{ sql }}
    ) as model_subq
  {%- else -%}

  {{ sql_header if sql_header is not none }}

  create table {{ relation.include(database=False) }}
  {%- if indexs is not none %} (
    {%- for index in indexs %}
      {%- set columns = index.get('columns') %}
    INDEX idx_{{ columns | replace(" ", "") | replace(",", "_") }} ({{ columns }}) USING BITMAP{{ "," if not loop.last }}
    {%- endfor %}
  )
  {%- endif -%}

  {%- if is_external -%}
    {{ starrocks__external_table() }}
  {%- elif engine == 'OLAP' -%}
    {{ starrocks__olap_table(True) }}
  {%- else -%}
    {%- set msg -%}
      "ENGINE = {{ engine }}" does not support, currently only supports 'OLAP'
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg) }}
  {%- endif -%}

  as {{ sql }}

  {%- endif -%}

{%- endmacro %}

{#- The model's YAML columns, with the table's key columns moved to the front: StarRocks
    requires key columns to lead the schema. Returns a list of column dicts. -#}
{% macro starrocks__contract_columns() -%}
  {%- set keys = starrocks__table_type_and_keys()['keys'] or [] -%}
  {%- set columns = model['columns'].values() | list -%}
  {%- set ordered = [] -%}
  {%- for key in keys -%}
    {%- for column in columns if column['name'] == key -%}
      {%- do ordered.append(column) -%}
    {%- endfor -%}
  {%- endfor -%}
  {%- for column in columns if column not in ordered -%}
    {%- do ordered.append(column) -%}
  {%- endfor -%}
  {{ return(ordered) }}
{%- endmacro %}

{% macro starrocks__contract_column_names() -%}
  {%- for column in starrocks__contract_columns() -%}
    {{ adapter.quote(column['name']) if column.get('quote') else column['name'] }}{{ ", " if not loop.last }}
  {%- endfor -%}
{%- endmacro %}

{#- CREATE TABLE with the column definitions and constraints declared in the model's YAML.
    Rendering the columns through the adapter also emits dbt's warnings for constraints
    that StarRocks does not support or enforce. -#}
{% macro starrocks__create_table_from_contract(relation) -%}
  {%- set indexs = config.get('indexs') -%}
  {%- set raw_columns = {} -%}
  {%- for column in starrocks__contract_columns() -%}
    {%- do raw_columns.update({column['name']: column}) -%}
  {%- endfor -%}
  {%- set column_defs = adapter.render_raw_columns_constraints(raw_columns=raw_columns) -%}
  {%- set model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
  {%- set definitions = column_defs + model_constraints -%}
  {%- if indexs is not none -%}
    {%- for index in indexs -%}
      {%- set columns = index.get('columns') -%}
      {%- do definitions.append("INDEX idx_" ~ (columns | replace(" ", "") | replace(",", "_")) ~ " (" ~ columns ~ ") USING BITMAP") -%}
    {%- endfor -%}
  {%- endif -%}

  create table {{ relation.include(database=False) }} (
    {%- for definition in definitions %}
    {{ definition }}{{ "," if not loop.last }}
    {%- endfor %}
  )
  {{ starrocks__olap_table(False) }}
{%- endmacro %}
