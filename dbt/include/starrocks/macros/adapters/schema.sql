{% macro starrocks__list_schemas(database) -%}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    select distinct schema_name from information_schema.schemata
    {%- endcall %}
    {{ return(load_result('list_schemas').table) }}
{%- endmacro %}

{% macro starrocks__create_schema(relation) -%}
    {% call statement('create_schema') %}
    create schema if not exists {{ relation.without_identifier() }}
    {% endcall %}
{%- endmacro %}

{% macro starrocks__drop_schema(relation) -%}
    {% call statement('drop_schema') %}
    drop schema if exists {{ relation.without_identifier() }}
    {% endcall %}
{%- endmacro %}

{% macro starrocks__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro starrocks__check_schema_exists(database, schema) -%}
{%- endmacro %}
