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

{% macro starrocks__drop_relation(relation) -%}
  {%- set relation_type = relation.type %}
  {%- if not relation_type or relation_type is none %}
      {%- set relation_type = 'table' %}
  {%- endif %}

  {% call statement('drop_relation', auto_begin=False) %}
    {%- if relation.is_materialized_view -%}
        drop materialized view if exists {{ relation }};
    {%- else -%}
        drop {{ relation_type }} if exists {{ relation }};
    {%- endif -%}
  {% endcall %}
{%- endmacro %}

{% macro starrocks__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') %}
    {% if from_relation.is_materialized_view and to_relation.is_materialized_view %}
        alter materialized view {{ from_relation }} rename {{ to_relation.table }}
    {%- elif from_relation.is_table and to_relation.is_table %}
       alter table {{ from_relation }} rename {{ to_relation.table }}
    {% elif from_relation.is_view and to_relation.is_view %}
      {% set results = run_query("select VIEW_DEFINITION as sql from information_schema.views where TABLE_SCHEMA='"
           + from_relation.schema + "' and TABLE_NAME='" + from_relation.table + "'") %}
      create view {{ to_relation }} as {{ results[0]['sql'] }}
      {% call statement('drop_view') %}
        drop view if exists {{ from_relation }}
      {% endcall %}
    {%- else -%}
      {%- set msg -%}
          unsupported rename from {{ from_relation.type }} to {{ to_relation.type }}
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}
  {% endcall %}
{%- endmacro %}

{% macro starrocks__exchange_relation(first_relation, second_relation) -%}
  {%- if second_relation.is_view %}
    {%- set from_results = run_query('show create view ' + first_relation.render() ) %}
    {%- set to_results = run_query('show create view ' + second_relation.render() ) %}
    {%- call statement('exchange_view_relation') %}
        alter view {{ relation1 }} as {{ to_results[0]['Create View'].split('AS',1)[1] }}
    {%- endcall %}
  {%- else %}
    {%- call statement('exchange_relation') %}
        alter table {{ first_relation }} swap with `{{ second_relation.table }}`;
    {%- endcall %}
  {%- endif %}
{%- endmacro %}
