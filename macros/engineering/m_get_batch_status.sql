{% macro m_get_batch_status(p_pipeline_name,batchid) %}
    {# Fetch the batch status using the pipeline name from DBT_MONITOR.dbt_model_batch_t, where the batch_status = 'running', for it to process the load, if the batch_status = 'completed', the load will be skipped #}
    {% call statement('get_batch_status', fetch_result=true) %}
        select 
		    batch_status
		from
			{{ source('dbt_config', 'dbt_batch_audit_t') }}
		where 
		    pipeline_name = '{{ p_pipeline_name }}'
			and batch_id = '{{ batchid }}'
    {% endcall %}

    {% if execute %}
        {%- set result = load_result('get_batch_status') -%}
        {% if result['data'] and result['data']|length > 0 %}
            {%- set batch_status = result['data'][0][0] -%}
            {{ return(batch_status) }}
        {% else %}
            {{ return("'00000000'") }}  {# default value if no result found #}
        {% endif %}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}