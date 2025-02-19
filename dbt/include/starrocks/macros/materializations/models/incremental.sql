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

{% macro get_incremental_insert_overwrite_sql(arg_dict) %}
      {% do return(get_insert_overwrite_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}
{% endmacro %}

{% macro get_incremental_dynamic_overwrite_sql(arg_dict) %}
      {% do return(get_dynamic_overwrite_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}
{% endmacro %}

{% macro _get_strategy_sql(target_relation, temp_relation, dest_cols_csv, is_dynamic_overwrite) %}
    {% set overwrite_type = "TRUE" if is_dynamic_overwrite else "FALSE" %}

    insert /*+SET_VAR(dynamic_overwrite = {{ overwrite_type }})*/ overwrite {{ target_relation }}({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ temp_relation }}
    )
{% endmacro %}

{% macro get_insert_overwrite_into_sql(target_relation, temp_relation, dest_columns) %}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- do return(_get_strategy_sql(target_relation, temp_relation, dest_cols_csv, false)) -%}
{% endmacro %}

{% macro get_dynamic_overwrite_into_sql(target_relation, temp_relation, dest_columns) %}
    {% if adapter.is_before_version("3.4.0") %}
        {%- set msg -%}
            [dynamic_overwrite] is only available from version 3.4.0 onwards, current version is {{ adapter.current_version() }}
        {%- endset -%}
        {{ exceptions.raise_compiler_error(msg) }}
    {% else %}
        {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
        {%- do return(_get_strategy_sql(target_relation, temp_relation, dest_cols_csv, true)) -%}
    {% endif %}
{% endmacro %}
