{% macro m_upd_dbt_model_pre_validation_check_on_success(model_grp, tgt_tbl, key_column_list, v_batch_id) %}
    {# This macro does the pre-validation checks and captures all the bad records , specifically related to the composite key(Null chk & Duplicate chk, or any other check defined) from the source , and writes in to a pre validation table dbt_monitor.dbt_model_pre_validation_chek_t #}
    {% set update_sql -%}
        begin;
        update 
            {{ source('dbt_audit','dbt_model_pre_validation_check_t') }} 
        set
            success_flg='Y'
        where 
            model_id = '{{ tgt_tbl }}'
            and model_grp = '{{ model_grp }}'
            and key_column in ( select distinct object_construct({% for column in key_column_list %} '{{ column }}',{{ column }} {% if not loop.last %},{% endif %} {% endfor %})::variant key_column
                            from {{ tgt_tbl }} src )
            and batch_id < '{{ v_batch_id }}' ;
        commit;
    {%- endset %}

    {% do run_query(update_sql) %}    
{% endmacro %}