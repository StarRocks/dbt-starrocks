{% macro starrocks__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
