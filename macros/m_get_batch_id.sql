{% macro m_get_batch_id(p_pipeline_name) %}
  {# Fetches the Batch Id from dbt_monitor.dbt_batch_audit_t by using the respective Pipeline name passed #}
  {% call statement('get_batch_id', fetch_result=true) %}
    select 
      batch_id
    from 
      {{ source('dbt_config', 'dbt_batch_audit_t') }}
    where 
      pipeline_name = '{{ p_pipeline_name }}'
      and batch_status = 'running'
  {% endcall %}
  {% if execute %}
    {%- set result = load_result('get_batch_id') -%}
    {%- if result['data'] and result['data']|length > 0 %}
      {%- set batch_id = result['data'][0][0] -%}
      {{ return(batch_id) }}
    {% else %}
      {{ return("00000000") }}  {# default value if no result found #}
    {% endif %}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}
