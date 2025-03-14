{%- macro starrocks__validate_microbatch_config(config) -%}
    {%- set required_config_keys = ['event_time', 'begin', 'batch_size'] -%}
    {%- for key in required_config_keys -%}
        {%- if not config.get(key) -%}
            {%- do exceptions.raise_compiler_error("The 'microbatch' incremental strategy requires the '" ~ key ~ "' configuration to be set.") -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}

{%- macro starrocks__validate_get_incremental_strategy(config) -%}
    {%- set strategy = config.get('incremental_strategy') or 'default' -%}
    {%- set invalid_strategy_msg -%}
        Invalid incremental strategy provided: {{ strategy }}
        Expected one of: 'default', 'insert_overwrite', 'dynamic_overwrite', 'microbatch'
    {%- endset -%}
    {%- if strategy not in ['default', 'insert_overwrite', 'dynamic_overwrite', 'microbatch'] -%}
        {%- do exceptions.raise_compiler_error(invalid_strategy_msg) -%}
    {%- endif -%}

    {%- if strategy == 'microbatch' -%}
        {%- do starrocks__validate_microbatch_config(config) -%}
    {%- endif -%}
    {%- do return (strategy) -%}
{%- endmacro -%}
