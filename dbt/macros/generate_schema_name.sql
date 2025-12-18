{#
  Multi-client schema routing macro
  
  Generates schema names based on custom schema
  The database is already set in profiles.yml, so we only need to return the schema name
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema | upper }}
    {%- else -%}
        {{ custom_schema_name | upper }}
    {%- endif -%}
{%- endmacro %}
