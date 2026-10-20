{#--
    Resolve `unique_key` to a list, or none when it is not set.
--#}
{%- macro starrocks__get_incremental_unique_key(config) -%}
    {%- set unique_key = config.get('unique_key') -%}
    {%- if unique_key is none -%}
        {%- do return(none) -%}
    {%- endif -%}
    {%- if unique_key is string -%}
        {%- do return([unique_key]) -%}
    {%- endif -%}
    {%- do return(unique_key) -%}
{%- endmacro -%}

{#--
    The StarRocks table type an incremental model's config builds: 'PRIMARY',
    'UNIQUE' or 'DUPLICATE'. Mirrors starrocks__olap_table, where `unique_key`
    promotes the target to a PRIMARY KEY table keyed on it.
--#}
{%- macro starrocks__incremental_table_type(config) -%}
    {%- if config.get('unique_key') is not none -%}
        {%- do return('PRIMARY') -%}
    {%- endif -%}
    {%- do return(config.get('table_type', 'DUPLICATE') | upper) -%}
{%- endmacro -%}

{%- macro starrocks__validate_microbatch_config(config) -%}
    {%- set required_config_keys = ['event_time', 'begin', 'batch_size'] -%}
    {%- for key in required_config_keys -%}
        {%- if not config.get(key) -%}
            {%- do exceptions.raise_compiler_error("The 'microbatch' incremental strategy requires the '" ~ key ~ "' configuration to be set.") -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}

{%- macro starrocks__validate_append_config(config) -%}
    {%- if starrocks__incremental_table_type(config) != 'DUPLICATE' -%}
        {%- set msg -%}
            The 'append' incremental strategy requires a DUPLICATE KEY table, but this model
            builds a {{ starrocks__incremental_table_type(config) }} KEY table
            {%- if config.get('unique_key') is not none %} (setting 'unique_key' makes it a PRIMARY KEY table){% endif -%}.
            An INSERT into a PRIMARY or UNIQUE KEY table replaces rows with matching keys rather
            than appending them. Use incremental_strategy='merge' to upsert.
        {%- endset -%}
        {%- do exceptions.raise_compiler_error(msg) -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro starrocks__validate_merge_config(config) -%}
    {%- if starrocks__incremental_table_type(config) != 'PRIMARY' -%}
        {%- set msg -%}
            The 'merge' incremental strategy requires a PRIMARY KEY table. Set 'unique_key', which
            becomes the table's PRIMARY KEY, or table_type='PRIMARY' with 'keys'.
        {%- endset -%}
        {%- do exceptions.raise_compiler_error(msg) -%}
    {%- endif -%}

    {#--
        StarRocks has no MERGE statement. The upsert is an INSERT INTO a PRIMARY KEY
        table, which always replaces the whole row, so per-column merge configs cannot
        be honoured. Fail loudly rather than silently ignoring them.
    --#}
    {%- for key in ['merge_update_columns', 'merge_exclude_columns'] -%}
        {%- if config.get(key) -%}
            {%- set msg -%}
                The 'merge' incremental strategy does not support '{{ key }}' on StarRocks.
                StarRocks has no MERGE statement: the upsert is an INSERT INTO a PRIMARY KEY
                table, which always replaces the whole row.
            {%- endset -%}
            {%- do exceptions.raise_compiler_error(msg) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}

{#--
    'default' is slated for deprecation. Announce it as an info log rather than a warning,
    so projects running with --warn-error are not broken before the deprecation lands.
--#}
{%- macro starrocks__log_default_strategy_notice(config) -%}
    {%- set msg -%}
        {%- if config.get('incremental_strategy') -%}
            incremental_strategy='default'
        {%- else -%}
            No incremental_strategy is set, so 'default' is used. It
        {%- endif %} will be deprecated in a future release ({{ model.unique_id }}).
        Its behaviour depends on the table type: it upserts into PRIMARY KEY tables and appends
        to DUPLICATE KEY tables. Set incremental_strategy='merge' for a PRIMARY KEY table or
        'append' for a DUPLICATE KEY table; both run the same SQL as 'default'.
    {%- endset -%}
    {%- do log(msg, info=True) -%}
{%- endmacro -%}

{%- macro starrocks__validate_get_incremental_strategy(config, is_external=false) -%}
    {%- set strategy = config.get('incremental_strategy') or 'default' -%}
    {%- set strategies = adapter.valid_incremental_strategies() -%}
    {%- set invalid_strategy_msg -%}
        Invalid incremental strategy provided: {{ strategy }}
        Expected one of: '{{ strategies | join("', '") }}'
    {%- endset -%}
    {%- if strategy not in strategies -%}
        {%- do exceptions.raise_compiler_error(invalid_strategy_msg) -%}
    {%- endif -%}

    {%- if is_external and strategy == 'merge' -%}
        {%- set msg -%}
            The '{{ strategy }}' incremental strategy is not supported for models in an external
            catalog: external tables have no PRIMARY KEY. Use 'insert_overwrite' or 'dynamic_overwrite'.
        {%- endset -%}
        {%- do exceptions.raise_compiler_error(msg) -%}
    {%- endif -%}

    {%- if strategy == 'default' -%}
        {%- do starrocks__log_default_strategy_notice(config) -%}
    {%- elif strategy == 'microbatch' -%}
        {%- do starrocks__validate_microbatch_config(config) -%}
    {%- elif strategy == 'append' and not is_external -%}
        {%- do starrocks__validate_append_config(config) -%}
    {%- elif strategy == 'merge' -%}
        {%- do starrocks__validate_merge_config(config) -%}
    {%- endif -%}
    {%- do return (strategy) -%}
{%- endmacro -%}

{#--
    The strategies above check the table type the config builds. An existing table can
    differ, e.g. one first built before 'unique_key' was set, and only a full refresh
    rebuilds it. Check the live table so the strategy cannot silently do something else.
--#}
{%- macro starrocks__validate_incremental_target_table_type(strategy, relation) -%}
    {%- set required = {
        'append': 'DUP_KEYS',
        'merge': 'PRIMARY_KEYS',
    }.get(strategy) -%}
    {%- if required is none -%}
        {%- do return(none) -%}
    {%- endif -%}

    {%- set actual = starrocks__get_table_model(relation) -%}
    {%- if actual is not none and actual != required -%}
        {%- set msg -%}
            The '{{ strategy }}' incremental strategy requires {{ relation }} to be a
            {{ required }} table, but it is a {{ actual }} table. If the model's config changed
            since the table was built, rebuild it with `dbt run --full-refresh`.
        {%- endset -%}
        {%- do exceptions.raise_compiler_error(msg) -%}
    {%- endif -%}
{%- endmacro -%}
