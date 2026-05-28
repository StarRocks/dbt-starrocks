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
{% macro starrocks__stored_view_definition(relation) -%}
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

{% materialization view, adapter='starrocks' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set sql = model['compiled_code'] -%}
  {%- set on_view_exists = config.get('on_view_exists', none) -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {# A non-view squatting on the target name (e.g. a table) must be dropped so a
     view can take its place; there is nothing to compare against. #}
  {%- if existing_relation is not none and not existing_relation.is_view -%}
    {{ adapter.drop_relation(existing_relation) }}
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
    {%- set backup_relation = make_backup_relation(target_relation, 'view') -%}
    {{ drop_relation_if_exists(load_cached_relation(intermediate_relation)) }}
    {{ drop_relation_if_exists(load_cached_relation(backup_relation)) }}

    {%- call statement('main') -%}
      {{ starrocks__create_view_as(intermediate_relation, sql) }}
    {%- endcall -%}

    {%- set existing_def = starrocks__stored_view_definition(existing_relation) -%}
    {%- set candidate_def = starrocks__stored_view_definition(intermediate_relation) -%}

    {%- if existing_def is not none and existing_def == candidate_def -%}
      {# Unchanged: drop the candidate and leave the existing view in place. #}
      {{ drop_relation_if_exists(intermediate_relation) }}
      {{ store_raw_result(name="main", message="skip " ~ target_relation, code="skip", rows_affected="-1") }}

    {%- elif on_view_exists == 'replace' -%}
      {# Changed: atomic replace. The candidate was only needed to compare. #}
      {{ drop_relation_if_exists(intermediate_relation) }}
      {%- call statement('main') -%}
        {{ starrocks__create_view_as(target_relation, sql) }}
      {%- endcall -%}

    {%- else -%}
      {# Changed: swap the candidate in via the backup/rename dance. #}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
      {{ adapter.rename_relation(intermediate_relation, target_relation) }}
      {{ drop_relation_if_exists(backup_relation) }}
    {%- endif -%}
  {%- endif -%}

  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) -%}
  {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}

  {%- do persist_docs(target_relation, model) -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
