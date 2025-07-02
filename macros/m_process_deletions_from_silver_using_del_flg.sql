{% macro m_process_deletions_from_silver_using_del_flg(p_pipeline_name, model_grp, model_id) %}
    {# Deletion of Silver Table records (Hard Delete), This macro compares and fetches all the deleted records by doing target-source,if the 
        records are deleted in stage by OGG, these delete records are then moved to tbl_del_t tables in the same silver layer, and then deleted from the target table. #}
    {# Prepares Target table using the full length target name , which is pulled from source yaml #}

    {# Fetch the Delete flag from the source yaml file, where it is Y or N using the model #}
    {% set del_flg = fl_utils.m_get_del_flg_using_model(model_grp, model_id) %}
    {# Fetch full length stage table name using the model id from source yaml #}
    {% set stg_tbl = fl_utils.m_get_stg_tbl_by_using_model(model_grp, model_id) %}
    {# Fetch Full length target table using the model id from yaml #}
    {% set tgt_tbl = fl_utils.m_get_tgt_tbl_by_using_model(model_grp, model_id) %}

    {% if del_flg == 'y' %}

        {% set fully_qualified_name_str = tgt_tbl | string %}
        {% set parts = fully_qualified_name_str.split('.') %}
        {% set tgt_db = parts[0] %}
        {% set tgt_schema = parts[1] %}
        {% set tgt_tbl_short = parts[2] %}

        {# Prepares deletion table name, using the Target table, which is of same structure of target #}
        {% set table_base = tgt_tbl_short[0:tgt_tbl_short|length - 2] %}
        {% set table_suffix = tgt_tbl_short[-2:] %}
        {% set del_table_name = table_base ~ '_del_' ~ table_suffix %}

        {# Fetch target table’s primary keys of target table #}
        {% call statement('fetch_primary_keys', fetch_result=true) %}
            show primary keys in table {{ tgt_db }}.{{ tgt_schema }}.{{ tgt_tbl_short }};
        {% endcall %}
        {% set pk_columns_result = load_result('fetch_primary_keys') %}
        {% set primary_key_columns = [] %}
        {% for col in pk_columns_result['data'] %}
            {% set _ = primary_key_columns.append(col[4] | lower) %}
        {% endfor %}
        {% do log("Primary Key columns:"~ primary_key_columns, info=True) %}
        {% set conditions = [] %}
        {% for key in primary_key_columns %}
            {% set condition = "t." ~ key ~ " = s." ~ key %}
            {% do conditions.append(condition) %}
        {% endfor %}
        {% set pk_join_condition = conditions | join(' and ') %}

        {# Step 0: Create the deletion table if doesn’t exist, using the same structure as the silver table #}
        {% set create_sql %}
            begin;
                create table if not exists {{ tgt_db }}.{{ tgt_schema }}.{{ del_table_name }}
                as select * from {{ tgt_tbl }} where 1=0;
            commit;
        {% endset %}
        {% do run_query(create_sql) %}

        {# Fetch target columns using the source yaml attribute for target table #}
        {% call statement('fetch_tgt_columns', fetch_result=true) %}
            select lower(column_name) as column_name
            from {{ tgt_db }}.information_schema.columns
            where lower(table_schema) = lower('{{ tgt_schema }}')
            and lower(table_name) = lower('{{ tgt_tbl_short }}')
        {% endcall %}
        {% set tgt_columns_result = load_result('fetch_tgt_columns') %}
        {% set tgt_columns_list = [] %}
        {% for col in tgt_columns_result['data'] %}
            {% set _ = tgt_columns_list.append(col[0]) %}
        {% endfor %}

        {# Columns exclusion for the schema comparison Source to Target, and filter out these columns, out of the comparison #}
        {% set columns_to_remove = ['src_load_ts','batch_id', 'etl_load_ts', 'etl_updt_ts'] %}
        {% set filtered_tgt_columns_list = [] %}
        {% for col in tgt_columns_list %}
            {% if col not in columns_to_remove %}
                {% set _ = filtered_tgt_columns_list.append(col) %}
            {% endif %}
        {% endfor %}
        {% set tgt_columns_list = filtered_tgt_columns_list %}
        {% set insert_columns_str = tgt_columns_list | join(', ') %}
        {% set select_columns_str = tgt_columns_list | join(', t.') %}

        {# Step 1: Identify records in the silver table that are not present in the stage table and add them to the deletion table #}
        {% set insert_sql %}
        begin;
            insert into {{ tgt_db }}.{{ tgt_schema }}.{{ del_table_name }} ({{ insert_columns_str }}, batch_id, src_load_ts, etl_load_ts, etl_updt_ts)
            select t.{{ select_columns_str }},
                   m.batch_id as batch_id
                  ,m.src_load_ts::timestamp_ntz(9) as src_load_ts
                  ,current_timestamp()::timestamp_ntz(9) as etl_load_ts
                  ,current_timestamp()::timestamp_ntz(9) as etl_updt_ts
            from {{ tgt_tbl }} t
            join (select max(batch_id) as batch_id, max(src_load_ts) as src_load_ts from {{ tgt_tbl }}) m
            where not exists (
                select 'x'
                from {{ stg_tbl }} s
                where {{ pk_join_condition }}
            );
        commit;
        {% endset %}
        {% do run_query(insert_sql) %}

        {# Step 2: Hard delete these records from the silver table #}
        {% set delete_sql %}
        begin;
            delete from {{ tgt_tbl }} t
            where exists (
                select 'x'
                from {{ tgt_db }}.{{ tgt_schema }}.{{ del_table_name }} s
                where {{ pk_join_condition }}
            );
        commit;
        {% endset %}
        {% do run_query(delete_sql) %}

    {% else %}
        {% do log("No Deletion applied to this model", info=True) %}
    {% endif %}
{% endmacro %}
