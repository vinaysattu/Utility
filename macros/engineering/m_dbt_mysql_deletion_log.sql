{% macro m_mysql_process_deletions_from_silver_using_model_id(p_pipeline_name, p_model_grp, p_model_id, p_batch_id) %}
    {%- set v_pipeline_name = p_pipeline_name -%}
    {%- set v_model_grp = p_model_grp -%}
    {%- set v_model_id = p_model_id -%}
    {%- set v_batch_id = p_batch_id -%}
    
    {% if execute %}
        {% set v_src_tbl = '' %}
        {% set v_tgt_tbl = '' %}
        {% set deletions_table = source('dbt_audit', 'dbt_mysql_deletion_log_t') %}
        
        {# Fetch the source yaml model attributes #}
        {% set sources = graph.get('sources', {}) %}
        
        {# Fetch the source yaml model attributes #}
        {% for source in graph.sources.values() %}
            {%- if source.source_name == v_model_grp -%}
                {%- if source.meta.tgt_tbl == v_model_id -%}
                    {%- set v_src_tbl = source.meta.src_archive_tbl -%}
                    {%- set v_tgt_tbl = source.meta.tgt_tbl -%}

                    {# Capturing all the Deleted PKs for each table into MySQL deletions tables #}
                    {% set capture_deletion_pk_sql -%}
                        begin;
                        merge into {{ deletions_table }} as tgt
                        using (
                            with src_tbl as (
                                select  
                                    *,
                                    row_number() over (partition by src.primary_key__pk order by src.seen_at desc, src.least_significant_position desc) as rnk
                                from {{ v_src_tbl }} src
                                where event_type = 'IncrementalDeleteRows'
                            )
                            select
                                '{{ v_tgt_tbl }}' as model_id,
                                '{{ v_model_grp }}' as model_grp,
                                '{{ v_pipeline_name }}' as pipeline_name,
                                src.primary_key__pk as pk,
                                src.seen_at as src_load_ts,
                                {{ v_batch_id }}::number(38, 0) as batch_id,
                                current_timestamp()::timestamp_ntz(9) as etl_load_ts,
                                current_timestamp()::timestamp_ntz(9) as etl_updt_ts
                            from src_tbl src
                            where rnk = 1
                        ) as src
                        on (src.model_id = tgt.model_id and src.pk = tgt.pk)
                        when matched then
                            update set tgt.model_grp = src.model_grp, tgt.pipeline_name = src.pipeline_name, tgt.etl_updt_ts = src.etl_updt_ts
                        when not matched then
                            insert (model_id, model_grp, pipeline_name, pk, src_load_ts, batch_id, etl_load_ts, etl_updt_ts)
                            values (src.model_id, src.model_grp, src.pipeline_name, src.pk, src.src_load_ts, src.batch_id, src.etl_load_ts, src.etl_updt_ts);
                        commit;
                    {%- endset %}
                    {% do run_query(capture_deletion_pk_sql) %}
                    
                    {# Fetch count of deleted records #}
                    {% set count_deleted_sql -%}
                        select count(1) as deleted_count from {{ v_tgt_tbl }}
                        where pk in (
                            select pk
                            from {{ deletions_table }}
                            where model_id = '{{ v_tgt_tbl }}'
                        );
                    {%- endset %}
                    
                    {% set deleted_count_result = run_query(count_deleted_sql) %}
                    
                    {% set rows_deleted = 0 %}
                    {% if deleted_count_result and deleted_count_result.columns[0].values is not none %}
                        {% set rows_deleted = deleted_count_result.columns[0].values()[0] %}
                    {% endif %}
                    {% do log("Current Batch Records Deleted: " ~ rows_deleted, info=True) %}
                    
                    {# Deleting the PKs from the target silver table, for PKs captured in the above step (mysql_deletions_t) #}
                    {% set delete_pk_sql -%}
                        begin;
                        delete from {{ v_tgt_tbl }}
                        where pk in (
                            select pk
                            from {{ deletions_table }}
                            where model_id = '{{ v_tgt_tbl }}'
                        );
                        commit;
                    {%- endset %}
                    {% do run_query(delete_pk_sql) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}


{% macro m_apply_typ1_deletion_on_tgt_model( p_src_tbl, p_tgt_tbl, p_src_key_column ) %}

	{%- set v_src_obj = p_src_tbl | string -%}
    {%- set v_tgt_tbl = p_tgt_tbl -%}
	{%- set v_src_key_column = p_src_key_column -%}
    {%- set v_src_tbl = v_src_obj[:-2] ~ '_t' if v_src_obj.endswith('_v') or v_src_obj.endswith('_V') else v_src_obj -%}

	{# Step 1: Hard delete these records from the Gold table #}
	{% set delete_sql -%}    
		begin;
		delete from {{ v_tgt_tbl }} t
		where {{ v_src_key_column }} in ( select distinct pk from {{ source('dbt_audit', 'dbt_mysql_deletion_log_t') }} s where upper(model_id) = upper('{{ v_src_tbl }}') );
		commit;
	{%- endset %}
	{% do run_query(delete_sql) %}  

{% endmacro %}


{% macro m_apply_typ2_deletion_on_tgt_model( p_src_tbl, p_tgt_tbl, p_src_key_column ) %}

	{%- set v_src_obj = p_src_tbl | string -%}
    {%- set v_tgt_tbl = p_tgt_tbl -%}
	{%- set v_src_key_column = p_src_key_column -%}
    {%- set v_src_tbl = v_src_obj[:-2] ~ '_t' if v_src_obj.endswith('_v') or v_src_obj.endswith('_V') else v_src_obj -%}

	{# Step 1: soft delete these records from the gold typ2 table #}
	{% set update_sql -%}    
		begin;
		update {{ v_tgt_tbl }} t
		set active_flg = 'N',
            end_ts = etl_updt_ts
		where active_flg = 'Y'
			and {{ v_src_key_column }} in ( select distinct pk from {{ source('dbt_audit', 'dbt_mysql_deletion_log_t') }} s where upper(model_id) = upper('{{ v_src_tbl }}') );
		commit;
	{%- endset %}
	{% do run_query(update_sql) %}  

{% endmacro %}