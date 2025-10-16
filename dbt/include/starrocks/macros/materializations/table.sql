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

{% materialization table, adapter='starrocks' %}
  
  {%- set target_relation = this.incorporate(type='table') -%}
  
  {%- set properties = config.get('properties', {}) -%}
  {%- set catalog = config.get('catalog', target.catalog) -%}
  {%- set database = config.get('database') or target_relation.schema -%}
  {%- set on_table_exists = config.get('on_table_exists', 'replace') -%}
  
  {%- if catalog == 'default_catalog' -%}
    {%- set is_external = false -%}
  {%- else -%}
    {%- if not catalog or not database -%}
      {%- set msg -%}
        External tables require both 'catalog' and 'database'
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
    {%- set is_external = true -%}
  {%- endif -%}
  
  {%- if is_external -%}
    {%- set target_relation = adapter.Relation.create(
        database=catalog,
        schema=database,
        identifier=target_relation.identifier,
        type='table'
    ) -%}
    {%- set existing_relation = load_relation(target_relation) -%}
    
  {%- else -%}
    {%- set existing_relation = load_cached_relation(this) -%}
  {%- endif -%}
  
  {%- set sql = model['compiled_code'] -%}
  
  {{ run_hooks(pre_hooks) }}
  
  {%- if existing_relation is not none -%}
    {%- if should_full_refresh() or on_table_exists == 'replace' -%}
      {{ log("Dropping and recreating table", info=True) }}
      {{ adapter.drop_relation(existing_relation) }}
      {%- set existing_relation = none -%}
      
    {%- elif on_table_exists == 'append' -%}
      {% call statement('main') -%}
        INSERT INTO {{ target_relation }}
        {{ sql }}
      {%- endcall %}
      {{ return({'relations': [target_relation]}) }}
      
    {%- elif on_table_exists == 'ignore' -%}
      {{ return({'relations': [target_relation]}) }}
      
    {%- else -%}
      {%- set msg -%}
        Unknown on_table_exists strategy: '{{ on_table_exists }}'.
        Valid options: 'replace', 'append', 'ignore'
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- endif -%}
  
  {%- if existing_relation is none -%}
    {{ log("Creating new table: " ~ target_relation, info=True) }}
    {% call statement('main') -%}
      {{ starrocks__create_table_as(false, target_relation, sql, is_external) }}
    {%- endcall %}
  {%- endif -%}
  
  {{ run_hooks(post_hooks) }}
  
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
