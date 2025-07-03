{% macro m_init_load_attrib_vals_for_all_models_in_dbt_model_audit(p_pipeline_name, modelgrp, model_id_list, batchid) %}
    {# initiate all the load attibutes (inc_load_ts, model_load_status,start_ts e.t.c..)  inside the table DBT_MONITOR.dbt_model_audit_t for the model group and the batch for the list of models #}
    {%- set full_tgt_tbl_list = [] -%}
    {# Formats the Model list and then prepares the model string, using the model list #}
    {%- for model_id in model_id_list -%}
        {% set model_id_full = fl_utils.m_get_tgt_tbl_by_using_model(modelgrp, model_id) %}
        {% do full_tgt_tbl_list.append("'" ~ model_id_full ~ "'") %}
    {%- endfor -%}
    {%- set full_model_id_list = full_tgt_tbl_list -%}
    {% set full_model_string = full_model_id_list | join(', ') %}

    {# Prepare each model record ready to insert (only for new models)  , which are in the model list #}
	{%- set union_sql = [] -%}
    {%- for modelid in full_model_id_list -%}
        {%- set query = "select " ~ modelid ~ " as model_id, '" ~ modelgrp ~ "' as model_grp, null as inc_load_ts, '" ~ p_pipeline_name ~ "' as pipeline_name, '" ~ batchid ~ "' as batch_id, 'yet to start' as model_load_status, null as rows_inserted, null as rows_updated, null as start_ts, null as end_ts" -%}
        {%- do union_sql.append(query) -%}
    {%- endfor -%}
    {%- set final_sql = union_sql | join(' union all ') -%}

    {% if full_model_id_list | length > 0 %}
        {# Merge a new set of models using the model list, and initiates all the attributes (inc_load_ts,model_load_status,start_ts,end_ts,e.t.c..) by using the last batch set of records #}
        {% set merge_sql -%}
            begin;
			merge into {{ source('dbt_config', 'dbt_model_audit_t') }} tgt
			using (
				select * 
				from ( {{ final_sql }} ) r
				where not exists
						(select 'x' 
						from {{ source('dbt_config', 'dbt_model_audit_t') }} c 
						where c.model_id=r.model_id
							and c.model_grp=r.model_grp
							and c.pipeline_name = r.pipeline_name
							and c.pipeline_name = '{{ p_pipeline_name }}' 
							and c.model_grp = '{{ modelgrp }}' 
							and c.model_id in ({{ full_model_string }}))
				union 
				select
					c.model_id, c.model_grp, c.inc_load_ts, '{{ p_pipeline_name }}' as pipeline_name, '{{ batchid }}' as batch_id, c.model_load_status, c.rows_inserted, c.rows_updated, c.start_ts, c.end_ts
				from {{ source('dbt_config', 'dbt_model_audit_t') }} c 
					join (select model_id,model_grp,max(batch_id) batch_id 
						from {{ source('dbt_config', 'dbt_model_audit_t') }} 
						group by all) cm 
						on (c.model_id=cm.model_id and c.model_grp=cm.model_grp and c.batch_id=cm.batch_id)
				where c.model_grp = '{{ modelgrp }}' 
					and c.pipeline_name = '{{ p_pipeline_name }}' 
					and c.model_id in ({{ full_model_string }})
				) src
			on (src.model_id = tgt.model_id 
				and src.model_grp = tgt.model_grp
				and src.batch_id = tgt.batch_id
				and tgt.model_grp = '{{ modelgrp }}'
				and tgt.pipeline_name = '{{ p_pipeline_name }}' 
				and tgt.model_id in ({{ full_model_string }}))
			when matched then
				update set 
				    tgt.model_load_status = case when tgt.end_ts is null then 'yet to start' else tgt.model_load_status end,
                    tgt.start_ts = case when tgt.end_ts is null then null else tgt.start_ts end,
                    tgt.end_ts = case when tgt.end_ts is null then null else tgt.end_ts end
			when not matched then
				insert (model_id, model_grp, inc_load_ts, pipeline_name, batch_id, model_load_status, rows_inserted, rows_updated,start_ts, end_ts)
				values (src.model_id, src.model_grp, src.inc_load_ts, src.pipeline_name,'{{ batchid }}', 'yet to start', null, null, null, null);
            commit;
        {%- endset %}
        {% do run_query(merge_sql) %}
	{% else %}
        {% do log("No models found in the model list or model grp to process", info=True) %}
    {% endif %}
   
{% endmacro %}


{% macro m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}
    
	{# initiate all the load attibutes (inc_load_ts, model_load_status,start_ts e.t.c..)  inside the table DBT_MONITOR.dbt_model_audit_t for the model and the batch for the model #}
    {# Merge a new set of models using the model list, and initiates all the attributes (inc_load_ts,model_load_status,start_ts,end_ts,e.t.c..) by using the last batch set of records #}
       
    {% set merge_sql -%}
        begin;
		merge into {{ source('dbt_config', 'dbt_model_audit_t') }} tgt
		using (
			select * 
			from ( select '{{ modelid }}' as model_id, '{{ modelgrp }}' as model_grp, null as inc_load_ts, '{{ p_pipeline_name }}' as pipeline_name, '{{ batchid }}' as batch_id, 'yet to start' as model_load_status, null as rows_inserted, null as rows_updated, null as start_ts, null as end_ts ) r
			where not exists
					(select 'x' 
					from {{ source('dbt_config', 'dbt_model_audit_t') }} c 
					where c.model_id=r.model_id
						and c.model_grp=r.model_grp
						and c.model_grp = '{{ modelgrp }}' 
						and c.pipeline_name = '{{ p_pipeline_name }}' 
						and c.model_id = '{{ modelid }}')
			union 
			select
				c.model_id, c.model_grp, c.inc_load_ts, '{{ p_pipeline_name }}' as pipeline_name, '{{ batchid }}' as batch_id, c.model_load_status, c.rows_inserted, c.rows_updated, c.start_ts, c.end_ts
			from {{ source('dbt_config', 'dbt_model_audit_t') }} c 
				join (select model_id,model_grp,max(batch_id) batch_id 
					from {{ source('dbt_config', 'dbt_model_audit_t') }} 
					group by all) cm 
					on (c.model_id=cm.model_id and c.model_grp=cm.model_grp and c.batch_id=cm.batch_id)
			where c.model_grp = '{{ modelgrp }}' 
				and c.pipeline_name = '{{ p_pipeline_name }}' 
				and c.model_id = '{{ modelid }}'
			) src
		on (src.model_id = tgt.model_id 
			and src.model_grp = tgt.model_grp
			and src.batch_id = tgt.batch_id
			and tgt.model_grp = '{{ modelgrp }}'
			and tgt.pipeline_name = '{{ p_pipeline_name }}' 
			and tgt.model_id = '{{ modelid }}')
		when matched then
			update set 
			    tgt.model_load_status = case when tgt.end_ts is null then 'running' else tgt.model_load_status end,
                tgt.start_ts = current_timestamp(),
                tgt.end_ts = null
		when not matched then
			insert (model_id, model_grp, inc_load_ts, pipeline_name,batch_id, model_load_status, rows_inserted, rows_updated,start_ts, end_ts)
			values (src.model_id, src.model_grp, src.inc_load_ts, src.pipeline_name, '{{ batchid }}', 'running', null, null, current_timestamp(), null);
        commit;
    {%- endset %}
    {% do run_query(merge_sql) %}

{% endmacro %}


{% macro m_get_inc_load_ts_for_model_record_from_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}
    {%- set select_query %}
        select concat('''', nvl(inc_load_ts, '1900-01-01'), '''') as source_parameter
        from {{ source('dbt_config', 'dbt_model_audit_t') }}
        where model_id = '{{ modelid }}'
          and model_grp = '{{ modelgrp }}'
		  and pipeline_name = '{{ p_pipeline_name }}' 
          and batch_id = '{{ batchid }}'
    {%- endset %}
 
    {% call statement('get_param_value', fetch_result=True) %}
        {{ select_query }}
    {% endcall %}
 
    {% if execute %}
        {%- set result = load_result('get_param_value') -%}
        {% if result['data'] and result['data'] | length > 0 %}
            {%- set parameter_value = result['data'][0][0] -%}
            {{ return(parameter_value) }}
        {% else %}
            {{ return("'1900-01-01'") }}  {# Default value if no result found #}
        {% endif %}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}


{% macro m_upd_pre_load_attrib_vals_for_model_record_in_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}

    {% set upd_sql -%}
        begin;
		update {{ source('dbt_config', 'dbt_model_audit_t') }} tg
        set tg.model_load_status = case when tg.end_ts is null then 'running' else tg.model_load_status end
            ,tg.rows_inserted = null
            ,tg.rows_updated = null
			,tg.start_ts=current_timestamp()
            ,tg.end_ts=null
        where tg.model_id= '{{ modelid }}'
			and tg.model_grp= '{{ modelgrp }}'
			and tg.pipeline_name = '{{ p_pipeline_name }}' 
            and tg.batch_id = '{{ batchid }}';
        commit;
    {%- endset %}        
	{% do run_query(upd_sql) %}

{% endmacro %}


{% macro m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid, p_inc_load_ts_column = 'src_load_ts' ) %}

    {% if execute %}

        {# Check if modelid is a relation object, and convert it to a string if needed #}
        {% if modelid is not string %}
            {% set modelid_db = modelid.database %}
        {% else %}
            {% set modelid_db = modelid.split('.')[0] %}
        {% endif %}

        {# fetch the query_id and query_text from snowflakes query history #}
        {% call statement('fetch_query_details', fetch_result=true) %}
		    select 
    			query_id,
			    replace(query_text,'''','''''') as query_text
		    from table({{ modelid_db }}.information_schema.query_history_by_user())
		    where 
    			end_time >= dateadd(second, -100, current_timestamp())
			    and query_text like '%{{ modelid }}%'
                and lower(query_text) not like '%dbt_monitor%'
			    and (lower(query_type) like 'create%' or lower(query_type) like 'merge%' or lower(query_type) like 'insert%' or lower(query_type) like 'copy%')
		    order by end_time desc
		    limit 1
        {% endcall %}  

        {% set result = load_result('fetch_query_details') %}
        {% if result['data'] and result['data']|length > 0 %}
            {# extract the query_id and query_text #}
            {% set query_id = result['data'][0][0] %}
            {% set query_text = result['data'][0][1] %}
			
            {% if modelgrp == 'mfcs_stage' %}
                {% set upd_sql -%}
    				begin;
				    update {{ source('dbt_config', 'dbt_model_audit_t') }} tg
				    set tg.inc_load_ts = null
    					,tg.model_load_status = 'completed'
					    ,tg.rows_inserted = rw.rows_inserted
					    ,tg.rows_updated = rw.rows_updated
					    ,tg.end_ts = current_timestamp()
					    ,tg.query_id = '{{ query_id }}'
                        ,tg.query_text = '{{ query_text }}'
				    from 
    					(select 
						    count(1) as rows_inserted,
						    0 as rows_updated
					    from {{ modelid }}
                        ) rw
    				where tg.model_id= '{{ modelid }}'
					    and tg.model_grp= '{{ modelgrp }}'
					    and tg.pipeline_name = '{{ p_pipeline_name }}' 
					    and tg.batch_id = '{{ batchid }}';
				    commit;
			    {%- endset %}        
			    {% do run_query(upd_sql) %}
            {% else %}
                    {% set upd_sql -%}
    				begin;
				    update {{ source('dbt_config', 'dbt_model_audit_t') }} tg
				    set tg.inc_load_ts = nvl(ts.{{ p_inc_load_ts_column }},tg.inc_load_ts)
    					,tg.model_load_status = 'completed'
					    ,tg.rows_inserted = rw.rows_inserted
					    ,tg.rows_updated = rw.rows_updated
					    ,tg.end_ts = current_timestamp()
					    ,tg.query_id = '{{ query_id }}'
                        ,tg.query_text = '{{ query_text }}'
				    from 
    					(select 
						    max({{ p_inc_load_ts_column }}) as {{ p_inc_load_ts_column }}				
					    from {{ modelid }}
    					where batch_id = '{{ batchid }}'
					    ) ts,
    					(select 
						    sum(case when etl_load_ts = etl_updt_ts then 1 else 0 end) as rows_inserted,
						    sum(case when etl_load_ts < etl_updt_ts then 1 else 0 end) as rows_updated
					    from {{ modelid }}
					    where batch_id = '{{ batchid }}'
                        ) rw
    				where tg.model_id= '{{ modelid }}'
					    and tg.model_grp= '{{ modelgrp }}'
						and tg.pipeline_name = '{{ p_pipeline_name }}' 
					    and tg.batch_id = '{{ batchid }}';
				    commit;
			    {%- endset %}        
			    {% do run_query(upd_sql) %}
            {% endif %}

        {% else %}
            {{ log("skipping execution as this is a dry run.", info=true) }}
        {% endif %}
    {% else %}
        {{ log("skipping execution as this is a dry run.", info=true) }}
    {% endif %}
{% endmacro %}


{% macro m_get_batch_id_for_model_record_from_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}

    {% call statement('get_param_value', fetch_result=true) %}
        select nvl(batch_id, 1900010101) as source_parameter
        from {{ source('dbt_config', 'dbt_model_audit_t') }}
        where model_id = '{{ modelid }}'
            and model_grp = '{{ modelgrp }}'
			and pipeline_name = '{{ p_pipeline_name }}' 
            and batch_id = '{{ batchid }}'
    {% endcall %}
    {% if execute %}
        {%- set result = load_result('get_param_value') -%}
        {% if result['data'] and result['data']|length > 0 %}
            {%- set parameter_value = result['data'][0][0] -%}
            {{ return(parameter_value) }}
        {% else %}
            {{ return("1900010101") }}  {# Default value if no result found #}
        {% endif %}
    {% else %}
        {{ return(false) }}
    {% endif %}

{% endmacro %}


{% macro m_get_load_stat_for_model_record_from_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}

    {% call statement('get_param_value', fetch_result=true) %}
        select model_load_status as source_parameter
        from {{ source('dbt_config', 'dbt_model_audit_t') }}
        where model_id = '{{ modelid }}'
            and model_grp = '{{ modelgrp }}'
			and pipeline_name = '{{ p_pipeline_name }}' 
            and batch_id = '{{ batchid }}'
    {% endcall %}
    {% if execute %}
        {%- set result = load_result('get_param_value') -%}
        {% if result['data'] and result['data']|length > 0 %}
            {%- set parameter_value = result['data'][0][0] -%}
            {{ return(parameter_value) }}
        {% else %}
            {{ return("none") }}  {# Default value if no result found #}
        {% endif %}
    {% else %}
        {{ return(false) }}
    {% endif %}

{% endmacro %}




{% macro m_get_rows_inserted_for_model_record_from_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}

    {% call statement('get_param_value', fetch_result=true) %}
        select nvl(rows_inserted, 0) as source_parameter
        from {{ source('dbt_config', 'dbt_model_audit_t') }}
        where model_id = '{{ modelid }}'
            and model_grp = '{{ modelgrp }}'
			and pipeline_name = '{{ p_pipeline_name }}' 
            and batch_id = '{{ batchid }}'
    {% endcall %}
    {% if execute %}
        {%- set result = load_result('get_param_value') -%}
        {% if result['data'] and result['data']|length > 0 %}
            {%- set parameter_value = result['data'][0][0] -%}
            {{ return(parameter_value) }}
        {% else %}
            {{ return(0) }}  {# Default value if no result found #}
        {% endif %}
    {% else %}
        {{ return(false) }}
    {% endif %}

{% endmacro %}

{% macro m_get_rows_updated_for_model_record_from_dbt_model_audit(p_pipeline_name, modelgrp, modelid, batchid) %}

    {% call statement('get_param_value', fetch_result=true) %}
        select nvl(rows_updated, 0) as source_parameter
        from {{ source('dbt_config', 'dbt_model_audit_t') }}
        where model_id = '{{ modelid }}'
            and model_grp = '{{ modelgrp }}'
			and pipeline_name = '{{ p_pipeline_name }}' 
            and batch_id = '{{ batchid }}'
    {% endcall %}
    {% if execute %}
        {%- set result = load_result('get_param_value') -%}
        {% if result['data'] and result['data']|length > 0 %}
            {%- set parameter_value = result['data'][0][0] -%}
            {{ return(parameter_value) }}
        {% else %}
            {{ return(0) }}  {# Default value if no result found #}
        {% endif %}
    {% else %}
        {{ return(false) }}
    {% endif %}

{% endmacro %}



