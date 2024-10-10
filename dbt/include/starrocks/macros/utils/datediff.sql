{% macro starrocks__datediff(first_date, second_date, datepart) -%}

    date_diff(
        '{{ datepart }}',
        {{ second_date }},
        {{ first_date }}
        )

{%- endmacro %}
