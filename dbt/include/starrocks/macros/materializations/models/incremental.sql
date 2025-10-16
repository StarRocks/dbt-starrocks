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

{%- macro get_incremental_insert_overwrite_sql(arg_dict) -%}
    {%- do return(get_insert_overwrite_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) -%}
{%- endmacro -%}

{%- macro get_incremental_dynamic_overwrite_sql(arg_dict) -%}
    {%- do return(get_dynamic_overwrite_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) -%}
{%- endmacro -%}

{%- macro starrocks__get_incremental_microbatch_sql(arg_dict) -%}
    {%- set microbatch_use_dynamic_overwrite = config.get('microbatch_use_dynamic_overwrite') or False -%}
    {%- if microbatch_use_dynamic_overwrite -%}
        {%- do return(get_incremental_dynamic_overwrite_sql(arg_dict)) -%}
    {%- else -%}
        {%- do return(get_incremental_insert_overwrite_sql(arg_dict)) -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro _get_strategy_sql(target_relation, temp_relation, dest_cols_csv, is_dynamic_overwrite) -%}
    {%- set overwrite_type = "TRUE" if is_dynamic_overwrite else "FALSE" %}

    insert /*+SET_VAR(dynamic_overwrite = {{ overwrite_type }})*/ overwrite {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ temp_relation }}
    )
{%- endmacro -%}

{%- macro get_insert_overwrite_into_sql(target_relation, temp_relation, dest_columns) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- do return(_get_strategy_sql(target_relation, temp_relation, dest_cols_csv, false)) -%}
{%- endmacro -%}

{%- macro get_dynamic_overwrite_into_sql(target_relation, temp_relation, dest_columns) -%}
    {%- if adapter.is_before_version("3.4.0") -%}
        {%- set msg -%}
            [dynamic_overwrite] is only available from version 3.4.0 onwards, current version is {{ adapter.current_version() }}
        {%- endset -%}
        {{ exceptions.raise_compiler_error(msg) }}
    {%- else -%}
        {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
        {%- do return(_get_strategy_sql(target_relation, temp_relation, dest_cols_csv, true)) -%}
    {%- endif -%}
{%- endmacro -%}

{%- materialization incremental, adapter='starrocks' -%}
    {%- set keys = config.get('keys', validator=validation.any[list]) -%}
    {%- set full_refresh_mode = (should_full_refresh()) -%}
    {%- set identifier = this.name -%}
    {%- set target_relation = api.Relation.create(
        identifier=identifier,
        schema=schema,
        database=database,
        type='table',
    ) -%}
    {%- set tmp_relation = make_temp_relation(this).incorporate(type='table') -%}

    {%- set catalog = config.get('catalog', target.catalog) -%}
    {%- set database = config.get('database') or target.schema -%}

    {%- if catalog != 'default_catalog' and database -%}
      {%- set is_external = true -%}
    {%- endif -%}

    {%- if is_external -%}
      {%- set target_relation = adapter.Relation.create(
        database=catalog,
        schema=database,
        identifier=target_relation.identifier,
        type='table'
      ) -%}
      {%- set tmp_relation = adapter.Relation.create(
        database=catalog,
        schema=database,
        identifier=tmp_relation.identifier,
        type='table'
      ) -%}
    {%- endif -%}

    {%- set existing_relation = load_relation(this) -%}
    {%- set incremental_strategy = starrocks__validate_get_incremental_strategy(config) -%}

    {{ drop_relation_if_exists(tmp_relation) }}

    {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

    {{ run_hooks(pre_hooks) }}

    {%- if existing_relation is none -%}
      {%- call statement('main') -%}
          {{ starrocks__create_table_as(False, target_relation, compiled_code, is_external) }}
      {%- endcall -%}

    {%- elif existing_relation.is_view -%}
        {#-- Can't overwrite a view with a table, drop it before creating table --#}
        {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
        {%- do adapter.drop_relation(existing_relation) -%}
        {%- call statement('main') -%}
            {{ starrocks__create_table_as(False, target_relation, compiled_code, is_external) }}
        {%- endcall -%}

    {%- elif full_refresh_mode -%}
        {#-- First create a backup of the existing table and then exchange the new table with the backup --#}
        {%- set backup_identifier = existing_relation.identifier ~ "__dbt_backup" -%}
        {%- set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) -%}
        {%- do adapter.drop_relation(backup_relation) -%}

        {%- call statement('main') -%}
            {{ starrocks__create_table_as(False, backup_relation, compiled_code, is_external) }}
        {%- endcall -%}

        {%- do starrocks__exchange_relation(target_relation, backup_relation) -%}
    {% else %}
        {#-- Create the temp relation, either as a view or as a temp table --#}
        {%- call statement('create_tmp_relation') -%}
            {{ starrocks__create_table_as(True, tmp_relation, compiled_code, is_external) }}
        {%- endcall -%}

        {%- do adapter.expand_target_column_types(
            from_relation=tmp_relation,
            to_relation=target_relation
        ) -%}
        {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {%- set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
        {%- if not dest_columns %}
            {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
        {%- endif -%}

        {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
        {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
        {%- set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) -%}
        {%- set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': tmp_relation, 'unique_key': keys, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) -%}

        {%- call statement('main') -%}
            {{ strategy_sql_macro_func(strategy_arg_dict) }}
        {%- endcall -%}
    {%- endif -%}

    {%- do drop_relation_if_exists(tmp_relation) -%}

    {{ run_hooks(post_hooks) }}

    {%- set target_relation = target_relation.incorporate(type='table') -%}

    {%- set should_revoke = should_revoke(existing_relation.is_table, full_refresh_mode) -%}
    {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}
    {%- do persist_docs(target_relation, model) -%}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
