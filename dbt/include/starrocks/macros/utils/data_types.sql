{# string  -------------------------------------------------     #}

{% macro starrocks__type_string() %}
    {{ return(api.Column.translate_type("string")) }}
{% endmacro %}


{# timestamp  -------------------------------------------------     #}

{% macro starrocks__type_timestamp() %}
    {{ return(api.Column.translate_type("datetime")) }}
{% endmacro %}


{# float  -------------------------------------------------     #}

{% macro starrocks__type_float() %}
    {{ return(api.Column.translate_type("float")) }}
{% endmacro %}

{# numeric  -------------------------------------------------     #}
{% macro starrocks__type_numeric() %}
    {{ return(api.Column.numeric_type("decimal", 28, 6)) }}
{% endmacro %}

{# bigint  -------------------------------------------------     #}

{% macro starrocks__type_bigint() %}
    {{ return(api.Column.translate_type("bigint")) }}
{% endmacro %}


{# int  -------------------------------------------------     #}

{%- macro starrocks__type_int() -%}
  {{ return(api.Column.translate_type("int")) }}
{%- endmacro -%}


{# bool  -------------------------------------------------     #}

{%- macro starrocks__type_boolean() -%}
  {{ return(api.Column.translate_type("boolean")) }}
{%- endmacro -%}
