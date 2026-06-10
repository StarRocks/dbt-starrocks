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

{% macro starrocks__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set on_view_exists = config.get('on_view_exists', none) -%}

  {{ sql_header if sql_header is not none }}

  {%- if on_view_exists == 'replace' -%}
    create or replace view {{ relation }} as {{ sql }};
  {%- else -%}
    create view {{ relation }} as {{ sql }};
  {%- endif -%}
{%- endmacro %}

{# Return the engine-stored definition for a view, or none if it is absent. #}
{% macro starrocks__stored_view_definition_internal(relation) -%}
  {%- set query -%}
    select view_definition as view_def
    from information_schema.views
    where table_schema = '{{ relation.schema }}'
      and table_name   = '{{ relation.identifier }}'
  {%- endset -%}
  {%- set result = run_query(query) -%}
  {%- if result.rows | length > 0 -%}
    {{ return(result[0]['view_def']) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}

{# Return the reconstructed DDL of a view in an external catalog (Iceberg,
   etc.), or none if it is absent. Uses `SHOW CREATE VIEW`, which routes
   through the connector layer and is catalog-aware. 
   Drop the whole PROPERTIES block today because the view
   materialization does not expose a user `properties` config (unlike
   `table`/`materialized_view`). #}
{% macro starrocks__stored_view_definition_external(relation) -%}
  {%- set query -%}
    show create view {{ relation }}
  {%- endset -%}
  {%- set result = run_query(query) -%}
  {%- if result.rows | length == 0 -%}
    {{ return(none) }}
  {%- endif -%}
  {# Column index 1 is the `Create View` column #}
  {%- set ddl = result[0][1] -%}
  {{ return(modules.re.sub('\\s*PROPERTIES\\s*\\([^)]*\\)', '', ddl)) }}
{%- endmacro %}

{% macro starrocks__stored_view_definition(relation) -%}
  {%- if starrocks__is_internal_catalog() -%}
    {{ return(starrocks__stored_view_definition_internal(relation)) }}
  {%- else -%}
    {{ return(starrocks__stored_view_definition_external(relation)) }}
  {%- endif -%}
{%- endmacro %}

{% materialization view, adapter='starrocks' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set sql = model['compiled_code'] -%}
  {%- set on_view_exists = config.get('on_view_exists', none) -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {# A non-view squatting on the target name (e.g. a table) must be dropped so a
     view can take its place; there is nothing to compare against. #}
  {%- set wrong_type_backup = none -%}
  {%- if existing_relation is not none and not existing_relation.is_view -%}
    {%- set wrong_type_backup = make_backup_relation(target_relation, existing_relation.type) -%}
    {{ drop_relation_if_exists(load_cached_relation(wrong_type_backup)) }}
    {{ adapter.rename_relation(existing_relation, wrong_type_backup) }}
    {%- set existing_relation = none -%}
  {%- endif -%}

  {%- if existing_relation is none -%}
    {%- call statement('main') -%}
      {{ starrocks__create_view_as(target_relation, sql) }}
    {%- endcall -%}

  {%- elif should_full_refresh() -%}
    {%- if on_view_exists != 'replace' -%}
      {{ adapter.drop_relation(existing_relation) }}
    {%- endif -%}
    {%- call statement('main') -%}
      {{ starrocks__create_view_as(target_relation, sql) }}
    {%- endcall -%}

  {%- else -%}
    {#
      Skip-when-unchanged, on every StarRocks version. Build the candidate under
      a temporary name and let the server canonicalize it, then compare its
      stored definition against the existing view.
    #}
    {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
    {{ drop_relation_if_exists(load_cached_relation(intermediate_relation)) }}

    {%- call statement('main') -%}
      {{ starrocks__create_view_as(intermediate_relation, sql) }}
    {%- endcall -%}

    {%- set existing_def = starrocks__stored_view_definition(existing_relation) -%}
    {%- set candidate_def = starrocks__stored_view_definition(intermediate_relation) -%}

    {# The candidate was only needed for comparison #}
    {{ drop_relation_if_exists(intermediate_relation) }}

    {%- if existing_def is not none and existing_def == candidate_def -%}
      {# Unchanged: leave the existing view in place. #}
      {{ store_raw_result(name="main", message="skip " ~ target_relation, code="skip", rows_affected="-1") }}

    {%- elif on_view_exists == 'replace' -%}
      {# atomic replace #}
      {%- call statement('main') -%}
        {{ starrocks__create_view_as(target_relation, sql) }}
      {%- endcall -%}

    {%- else -%}
      {# drop the existing view, then create at the target name. #}
      {{ adapter.drop_relation(existing_relation) }}
      {%- call statement('main') -%}
        {{ starrocks__create_view_as(target_relation, sql) }}
      {%- endcall -%}
    {%- endif -%}
  {%- endif -%}

  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) -%}
  {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}

  {%- do persist_docs(target_relation, model) -%}

  {%- if wrong_type_backup is not none -%}
    {{ drop_relation_if_exists(wrong_type_backup) }}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
