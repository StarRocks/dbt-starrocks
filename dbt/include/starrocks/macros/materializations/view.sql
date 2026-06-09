{% materialization view, adapter='starrocks' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='view') -%}

  {#- Look up persist_docs configuration flags -#}
  {%- set persist_relation = config.get('persist_docs', {}).get('relation', false) -%}
  {%- set persist_columns = config.get('persist_docs', {}).get('columns', false) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- Drop the old view if it exists, exactly how the adapter natively does it
  {% if old_relation is not none %}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  -- Create the fresh view with comments baked straight into the DDL
  {% call statement('main') -%}
    CREATE VIEW {{ target_relation.include(database=False) }}
    
    {%- if persist_columns and model.columns | length > 0 -%}
      (
      {%- for col in model.columns.values() %}
        {{ adapter.quote(col.name) }}{% if col.description %} COMMENT {{ dbt.string_literal(col.description) }}{% endif %}{{ "," if not loop.last }}
      {%- endfor %}
      )
    {%- endif %}

    {%- if persist_relation and model.description -%}
      COMMENT {{ dbt.string_literal(model.description) }}
    {%- endif %}
    
    AS {{ sql }};
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
