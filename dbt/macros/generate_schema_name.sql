{#
  Multi-client schema routing macro
  
  Generates schema names based on client + environment + custom schema
  Example: ACME_DEV_RAW.STAGING or ACME_PROD_RAW.MARTS
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set client_name = var('client_name', 'default') | upper -%}
    {%- set env = target.name | upper -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ client_name }}_{{ env }}_RAW.{{ default_schema | upper }}
    {%- else -%}
        {{ client_name }}_{{ env }}_RAW.{{ custom_schema_name | upper }}
    {%- endif -%}
{%- endmacro %}
