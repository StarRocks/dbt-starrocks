{% macro basic_load_csv_rows(model, batch_size, agate_table) -%}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
    {% set bindings = [] %}

    {% for row in chunk %}
    {% do bindings.extend(row) %}
    {% endfor %}

    {% set sql %}
    insert into {{ this.render() }} ({{ cols_sql }}) values
            {% for row in chunk %}
                ({% for column in agate_table.column_names %}
                    %s
                    {% if not loop.last%},{% endif %}
                {% endfor %})
                {% if not loop.last%},{% endif %}
    {% endfor %}
    {% endset %}

    {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

    {% if loop.index0 == 0 %}
    {% do statements.append(sql) %}
    {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{%- endmacro %}


{% macro starrocks__create_csv_table(model, agate_table) -%}
    {% set column_override = model['config'].get('column_types', {}) %}
    {% set quote_seed_column = model['config'].get('quote_columns', None) %}

    {% set sql %}
    create table {{ this.render() }}
    (
        {% for col_name in agate_table.column_names %}
        {% set inferred_type = adapter.convert_type(agate_table, loop.index0) %}
        {% set type = column_override.get(col_name, inferred_type) %}
        {% set column_name = (col_name | string) %}
        {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    {{ starrocks__engine() }}
    {{ starrocks__duplicate_key() }}
    {{ starrocks__partition_by() }}
    {{ starrocks__distributed_by(agate_table.column_names) }}
    {{ starrocks__properties() }}
    {% endset %}

    {% call statement('_') %}
    {{ sql }}
    {% endcall %}

    {{ return(sql) }}

{%- endmacro %}
