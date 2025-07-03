{% macro m_batch_id_calc_n_ins(p_pipeline_name) %}
    {# Creates new batch id using the current timestamp, remains unchanged untill all the load of pipeline is finished and concluded, only then a new batch id can be started/opened #}
    {# This macro should only be initiated at the very beginning of respective pipeline #}
    {% call statement('calculate_batch_id', fetch_result=True) %}
        with dt as
        (
            select current_timestamp()::timestamp_ntz as batch_time
        ),
        batch_id_calc as
        (
            select
                to_number(to_char(batch_time, 'yyyymmddhh24missff')) as batch_id,
                batch_time
            from dt
        )
        select
            '{{ p_pipeline_name }}' as pipeline_name,
            batch_id,
            'running' as batch_status,
            current_timestamp() as start_ts,
            null as end_ts
        from batch_id_calc
        where 0 = (
            select count(1)
            from {{ source('dbt_config','dbt_batch_audit_t') }}
            where pipeline_name = '{{ p_pipeline_name }}'
                and batch_status != 'completed'
        )
    {% endcall %}
   
    {% if execute %}
        {%- set result = load_result('calculate_batch_id') -%}
        {% if result['data'] and result['data']|length > 0 %}
            {%- set pipeline_name = result['data'][0][0] -%}
            {%- set batch_id = result['data'][0][1] -%}
            {%- set batch_status = result['data'][0][2] -%}
            {%- set start_ts = result['data'][0][3] -%}
            {%- set end_ts = result['data'][0][4] -%}
        
            {{ fl_utils.m_batch_merge(pipeline_name, batch_id, batch_status, start_ts) }}
          
        {% else %}
            {{ log("No batch record found to insert.", info=True) }}
        {% endif %}
    {% else %}
        {{ log("Batch ID calculation was not executed.", info=True) }}
    {% endif %}
{% endmacro %}

{% macro m_batch_merge(pipeline_name, batch_id, batch_status, start_ts) %}
    {% set merge_sql %}
        begin;
        merge into {{ source('dbt_config','dbt_batch_audit_t') }} as target
        using (
            select '{{ pipeline_name }}' as pipeline_name, {{ batch_id }} as batch_id, '{{ batch_status }}' as batch_status, '{{ start_ts }}' as start_ts, null as end_ts
        ) as source
        on (target.pipeline_name = source.pipeline_name and target.batch_id = source.batch_id)
        when matched then
            update set
                target.end_ts = null
        when not matched then
            insert (pipeline_name, batch_id, batch_status, start_ts, end_ts)
            values (
                source.pipeline_name,
                source.batch_id,
                source.batch_status,
                source.start_ts,
                source.end_ts
            );
        commit;
    {% endset %}
    
    {% do run_query(merge_sql) %}
{% endmacro %}


{% macro m_batch_id_update_status_to_completed(p_pipeline_name) %}
    {# This macro is used to close the current running batch, by updating the dbt_monitor.dbt_model_batch_id record to completed #}
    {# This macro should only be initiated at the very end of respective pipeline #}
    {% set upd_sql -%}
        begin;
        update {{ source('dbt_config', 'dbt_batch_audit_t') }}
        set 
            batch_status = 'completed',
            end_ts = current_timestamp()
        where 
            pipeline_name = '{{ p_pipeline_name }}'
            and batch_status = 'running';
        commit;
    {%- endset %}

    {% do run_query(upd_sql) %}           
{% endmacro %}
