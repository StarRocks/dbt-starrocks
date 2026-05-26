{% macro starrocks__normalize_sql(s) -%}
  {# Strip SQL comments and blank lines so a stored view/MV definition can be
     compared against compiled SQL. Shared by the view and materialized_view
     materializations. #}
  {%- set ns = namespace(lines=[]) -%}
  {%- for raw in s.split('\n') -%}
    {%- set line = raw.strip() -%}
    {%- if line.startswith('--') -%}
    {%- elif '--' in line -%}
      {%- set trimmed = line[:line.find('--')].strip() -%}
      {%- if trimmed -%}{%- do ns.lines.append(trimmed) -%}{%- endif -%}
    {%- elif line -%}
      {%- do ns.lines.append(line) -%}
    {%- endif -%}
  {%- endfor -%}
  {{- ns.lines | join('\n') -}}
{%- endmacro %}
