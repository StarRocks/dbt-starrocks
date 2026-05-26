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

{% macro starrocks__drop_view(relation) -%}
  {%- set on_view_exists = config.get('on_view_exists', none) -%}
  {%- if on_view_exists == 'replace' -%}
  {%- else -%}
    drop view if exists {{ relation.render() }}
  {%- endif -%}
{%- endmacro %}

{% materialization view, adapter='starrocks' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set sql = model['compiled_code'] -%}
  {%- set on_view_exists = config.get('on_view_exists', none) -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {# Skip-when-unchanged (StarRocks >= 4.0.6 only). #}
  {%- if existing_relation is not none
         and existing_relation.is_view
         and not adapter.is_before_version("4.0.6") -%}
    {%- set view_query -%}
      select view_definition as view_def
      from information_schema.views
      where table_schema = '{{ existing_relation.schema }}'
        and table_name   = '{{ existing_relation.identifier }}'
    {%- endset -%}
    {%- set stored = run_query(view_query) -%}
    {%- if stored.rows | length > 0
           and starrocks__normalize_sql(stored[0]['view_def']) == starrocks__normalize_sql(sql) -%}
      {{ store_raw_result(name="main", message="skip " ~ target_relation, code="skip", rows_affected="-1") }}
      {{ run_hooks(post_hooks) }}
      {{ return({'relations': [target_relation]}) }}
    {%- endif -%}
  {%- endif -%}

  {#
    Build / replace path. Reached when the view is new, its SQL changed, the
    server is < 4.0.6, or the target name is held by a non-view relation.
  #}

  {# If the target name is held by a non-view (e.g. a table), drop it so the
     view can take its place. #}
  {%- if existing_relation is not none and not existing_relation.is_view -%}
    {{ adapter.drop_relation(existing_relation) }}
    {%- set existing_relation = none -%}
  {%- endif -%}

  {%- if on_view_exists == 'replace' -%}
    {%- call statement('main') -%}
      {{ starrocks__create_view_as(target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}
    {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
    {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {%- set backup_relation = make_backup_relation(target_relation, 'view') -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}

    {%- call statement('main') -%}
      {{ starrocks__create_view_as(intermediate_relation, sql) }}
    {%- endcall -%}

    {%- if existing_relation is not none -%}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {%- endif -%}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

    {{ drop_relation_if_exists(backup_relation) }}
  {%- endif -%}

  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) -%}
  {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}

  {%- do persist_docs(target_relation, model) -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}