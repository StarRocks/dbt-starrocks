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