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

{% macro starrocks__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale

    from INFORMATION_SCHEMA.columns
    where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and table_schema = '{{ relation.schema }}'
      {% endif %}
    order by ordinal_position
  {% endcall %}

  {% call statement('desc_columns_in_relation', fetch_result=True) %}
        desc `{{ relation.schema }}`.`{{ relation.identifier }}`
  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {% set desc_table = load_result('desc_columns_in_relation').table %}

  {{ return(starrocks__sql_convert_columns_in_relation(relation, table, desc_table)) }}
{% endmacro %}

{% macro starrocks__sql_convert_columns_in_relation(relation, table, desc_table) -%}
  {% do relation.init_type_map(desc_table) %}
  {% set columns = [] %}
  {% for row in table %}
    -- rows[1] means type from information_schema
    {% if row[1] in ['array', 'struct', 'map'] %}
        {% set fixed_row = relation.get_type_by_desc(row) %}
        {% do columns.append(api.Column(*fixed_row)) %}
    {%- else -%}
        {% do columns.append(api.Column(*row)) %}
    {% endif %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}
