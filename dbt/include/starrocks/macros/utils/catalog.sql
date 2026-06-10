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

{# Return the catalog the current model lives in: model-level
   `{{ config(catalog=...) }}` wins, otherwise use profile `target.catalog`
   (which defaults to `default_catalog`). #}
{% macro starrocks__catalog_for() -%}
  {{ return(config.get('catalog', target.catalog)) }}
{%- endmacro %}

{# True when the current model lives in StarRocks's internal catalog
   (`default_catalog`). External catalogs (Iceberg, Hive, JDBC, ...) often
   need a different code path. #}
{% macro starrocks__is_internal_catalog() -%}
  {{ return(starrocks__catalog_for() == 'default_catalog') }}
{%- endmacro %}
