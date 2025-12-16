{#
  Adds query tags for cost attribution and monitoring
  
  Tags include: client, environment, model name, run timestamp
#}

{% macro set_query_tag() -%}
    {%- if var('query_tag_enabled', true) -%}
        {%- set client = var('client_name', 'unknown') -%}
        {%- set env = target.name -%}
        {%- set model_name = this.name if this is defined else 'unknown' -%}
        {%- set run_id = invocation_id -%}
        
        ALTER SESSION SET QUERY_TAG = '{"client": "{{ client }}", "env": "{{ env }}", "model": "{{ model_name }}", "run_id": "{{ run_id }}"}';
    {%- endif -%}
{%- endmacro %}
