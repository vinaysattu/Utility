{% macro m_insupd_dbt_model_pre_validation_check_on_composite_key(model_grp, src_tbl, tgt_tbl, key_column_list, src_json_column, v_batch_id) %}
    {# This macro does the pre-validation checks and captures all the bad records , specifically related to the composite key(Null chk & Duplicate chk, or any other check defined) from the source , and writes in to a pre validation table dbt_monitor.dbt_model_pre_validation_chek_t #}
    {% set merge_sql -%}
        begin;
        merge into {{ source('dbt_audit','dbt_model_pre_validation_check_t') }} as target
        using (
            with json_data_preparation as (
                select
                    '{{ tgt_tbl }}'::varchar(2048) as model_id
                    ,'{{ model_grp }}'::varchar(2048) as model_grp
                    ,object_construct({% for column in key_column_list %} '{{ column }}',{{ column }} {% if not loop.last %},{% endif %} {% endfor %})::variant key_column
                    ,'Duplication Check for Key Columns' validation_type
                    ,count(1)::number(38,0) as issue_record_count
                    ,case when {{ src_json_column }} ='none' then object_construct(*) else {{ src_json_column }} end ::variant json_value
                    ,'N' as success_flg
                    ,'{{ v_batch_id }}'::number(38,0) as batch_id
                    ,src.src_load_ts::timestamp_ntz as src_load_ts
                    ,current_timestamp::timestamp_ntz as etl_load_ts
                    ,current_timestamp::timestamp_ntz as etl_updt_ts
                from {{ src_tbl }} src
                group by all
                having count(1) > 1
                union all
                select
                    '{{ tgt_tbl }}'::varchar(2048) as model_id
                    ,'{{ model_grp }}'::varchar(2048) as model_grp
                    ,object_construct({% for column in key_column_list %} '{{ column }}',{{ column }} {% if not loop.last %},{% endif %} {% endfor %})::variant key_column
                    ,'Null Check for Key Columns' validation_type
                    ,count(1)::number(38,0) as issue_record_count
                    ,case when {{ src_json_column }} ='none' then object_construct(*) else {{ src_json_column }} end::variant json_value
                    ,'N' as success_flg
                    ,'{{ v_batch_id }}'::number(38,0) as batch_id
                    ,src.src_load_ts::timestamp_ntz as src_load_ts
                    ,current_timestamp::timestamp_ntz as etl_load_ts
                    ,current_timestamp::timestamp_ntz as etl_updt_ts
                from {{ src_tbl }} src
                where {% for column in key_column_list %} {{ column }} is null {% if not loop.last %} or {% endif %} {% endfor %}
                group by all
            )
            select * from json_data_preparation
        ) as source
        on (target.model_id = source.model_id and target.model_grp = source.model_grp and target.key_column = source.key_column and target.validation_type = source.validation_type and target.src_load_ts=source.src_load_ts)
        when matched then
            update set
                target.validation_type = source.validation_type,
                target.issue_record_count = source.issue_record_count,
                target.json_value = source.json_value,
                target.batch_id = source.batch_id,
                target.etl_updt_ts = current_timestamp::timestamp_ntz
        when not matched then
            insert (model_id, model_grp, key_column, validation_type, issue_record_count, json_value, success_flg,batch_id, src_load_ts, etl_load_ts, etl_updt_ts)
            values (source.model_id, source.model_grp, source.key_column, source.validation_type, source.issue_record_count, source.json_value, source.success_flg, source.batch_id, source.src_load_ts, current_timestamp::timestamp_ntz, current_timestamp::timestamp_ntz);
        commit;
    {%- endset %}

    {% do run_query(merge_sql) %}    
{% endmacro %}