{% macro starrocks__create_table_as(temporary, relation, sql) -%}
  {% set sql_header = config.get('sql_header', none) %}

  {{ sql_header if sql_header is not none }}
  create table {{ relation.include(database=False) }} as {{ sql }}
{%- endmacro %}