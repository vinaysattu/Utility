{% macro m_dynamic_incload_mysql_stage_to_silver_using_model_id(p_pipeline_name, p_model_grp, p_model_id, p_inc_load_ts, p_batch_id) %}
    {% do log("Initiating the load from MySql Stage to Silver for the model: " ~ p_model_id, info=True) %}
	
    {%- set v_pipeline_name = p_pipeline_name -%}
    {%- set v_model_grp = p_model_grp -%}
	{%- set v_model_id = p_model_id -%}
	{%- set v_inc_load_ts = p_inc_load_ts -%}
	{%- set v_batch_id = p_batch_id -%}
    {% if execute %}
        {%- set v_src_tbl = '' -%}
        {%- set v_tgt_tbl = '' -%}
        {%- set v_load_type = '' -%}
        {%- set v_inc_load_ts_column = '' -%}
        {%- set v_primary_key_columns = [] -%}  
        {# Fetch the source yaml model attibutes #}
        {% for source in graph.sources.values() %}
            {%- if source.source_name == v_model_grp -%}
                {%- if source.meta.tgt_tbl == v_model_id -%}
                    {%- set v_src_tbl = source.meta.src_tbl -%}
                    {%- set v_tgt_tbl = source.meta.tgt_tbl -%}
                    {%- set v_load_type = source.meta.load_type -%}
                    {%- set v_inc_load_ts_column = source.loaded_at_field -%}
                    {% do log("Setting src_tbl: " ~ v_src_tbl ~ ", tgt_tbl: " ~ v_tgt_tbl ~ ", load_type: " ~ v_load_type ~ ", inc_load_ts_column: " ~ v_inc_load_ts_column, info=True) %}

                    {# Fetch column information for source and target tables for source yaml #}
                    {% set v_src_db = v_src_tbl.split('.')[0] %}
                    {% set v_src_schema = v_src_tbl.split('.')[1] %}
                    {% set v_src_table = v_src_tbl.split('.')[2] %}
                    {% set v_tgt_db = v_tgt_tbl.split('.')[0] %}
                    {% set v_tgt_schema = v_tgt_tbl.split('.')[1] %}
                    {% set v_tgt_table = v_tgt_tbl.split('.')[2] %}

                    {# Fetch source columns using the source yaml attribute for source table #}
                    {% call statement('fetch_src_columns', fetch_result=true) %}
                        select lower(column_name) as column_name
                        from {{ v_src_db }}.information_schema.columns
                        where lower(table_schema) = replace(lower('{{ v_src_schema }}'),'"','')
                          and lower(table_name) = lower('{{ v_src_table }}')
                    {% endcall %}
                    {% set src_columns_result = load_result('fetch_src_columns') %}
                    {% set src_columns_list = [] %}
                    {% for col in src_columns_result['data'] %}
                        {% set _ = src_columns_list.append(col[0]) %}
                    {% endfor %}

                    {# Fetch target columns using the source yaml attribute for target table #}
                    {% call statement('fetch_tgt_columns', fetch_result=true) %}
                        select lower(column_name) as column_name
                        from {{ v_tgt_db }}.information_schema.columns
                        where lower(table_schema) = lower('{{ v_tgt_schema }}')
                          and lower(table_name) = lower('{{ v_tgt_table }}')
                    {% endcall %}
                    {% set tgt_columns_result = load_result('fetch_tgt_columns') %}
                    {% set tgt_columns_list = [] %}
                    {% for col in tgt_columns_result['data'] %}
                        {% set _ = tgt_columns_list.append(col[0]) %}
                    {% endfor %}

                    {# Fetch target tables primary keys #}
                    {% call statement('fetch_primary_keys', fetch_result=true) %}
                        show primary keys in table {{ v_tgt_db }}.{{ v_tgt_schema }}.{{ v_tgt_table }};
                    {% endcall %}
                    {% set pk_columns_result = load_result('fetch_primary_keys') %}
                    {% set v_primary_key_columns = [] %}
                    {% for col in pk_columns_result['data'] %}
                        {% set _ = v_primary_key_columns.append(col[4] | lower) %}
                    {% endfor %}
                    {% set conditions = [] %}
	                {% for key in v_primary_key_columns %}
                        {% set condition = "src." ~ key ~ " = tgt." ~ key %}
                        {% do conditions.append(condition) %}
                    {% endfor %}
                    {% set pk_join_condition = conditions | join(' and ') %}

                    {# Columns exclusion for the schema comaparsion Soruce to Target, and filter out these columns, out of the comaprison #}
                    {%- set columns_to_remove = ['src_load_ts','batch_id', 'etl_load_ts', 'etl_updt_ts'] -%}
                    {%- set filtered_tgt_columns_list = [] -%}
                    {%- for col in tgt_columns_list -%}
                        {%- if col not in columns_to_remove -%}
                            {%- set _ = filtered_tgt_columns_list.append(col) -%}
                        {%- endif -%}
                    {%- endfor -%}
                    {%- set tgt_columns_list = filtered_tgt_columns_list -%}

                    {%- set filtered_src_columns_list = [] -%}
                    {%- for col in src_columns_list -%}
                        {%- if col not in columns_to_remove -%}
                            {%- set _ = filtered_src_columns_list.append(col) -%}
                        {%- endif -%}
                    {%- endfor -%}
                    {%- set src_columns_list = filtered_src_columns_list -%}   

                    {{ fl_utils.m_capture_schema_drift_for_model_id(v_pipeline_name, v_model_grp, v_model_id, v_batch_id) }}

                    {% set replication_status = fl_utils.m_mysql_agent_check(v_model_grp, v_src_table) %}
                    {% if replication_status != 'running' %}

						{# SQL preperation of Source & Target coulmns #}
						{% set pk_columns_str = v_primary_key_columns | join(', ') %}
						{% set insert_columns_str = tgt_columns_list | join(', ') %}
						{% set select_columns_str = tgt_columns_list | join(', src.') %}
						{%- set update_columns_list = [] -%}
						{%- for col in tgt_columns_list -%}
							{%- if col not in v_primary_key_columns -%}
								{%- set _ = update_columns_list.append(col) -%}
							{%- endif -%}
						{%- endfor -%}
						{% set update_conditions = [] %}
						{% for col in update_columns_list %}
							{% set update_condition = "tgt." ~ col ~ " = src." ~ col %}
							{% do update_conditions.append(update_condition) %}
						{% endfor %}
						{% set update_columns_str = update_conditions | join(' , ') %}
	
						{# The Incremental (New batch) data load is then merged in to the Target #}
						{% set merge_sql -%}
							begin;
							merge into {{ v_tgt_tbl }} as tgt
							using (
								select 
									src.{{ select_columns_str }},
                                    src.{{ v_inc_load_ts_column }} as src_load_ts,
									{{ v_batch_id }}::number(38, 0) as batch_id,
									current_timestamp()::timestamp_ntz(9) as etl_load_ts,
									current_timestamp()::timestamp_ntz(9) as etl_updt_ts
								from {{ v_src_tbl }} src
                                {% if v_load_type != 'full' %}
									    join (select {{ pk_columns_str}} ,max({{ v_inc_load_ts_column }}) as {{ v_inc_load_ts_column }} from {{ v_src_tbl }} group by all) tgt on ( {{ pk_join_condition }} and src.{{ v_inc_load_ts_column }}=tgt.{{ v_inc_load_ts_column }} )
								    where src.{{ v_inc_load_ts_column }} > {{ v_inc_load_ts }}
                                {% endif %}
								) as src
								on ( {{ pk_join_condition }} )
							when matched then
								update set
									{{ update_columns_str }},
                                    tgt.src_load_ts=src.src_load_ts,
									tgt.batch_id = src.batch_id,
									tgt.etl_load_ts = src.etl_load_ts,
									tgt.etl_updt_ts = src.etl_updt_ts
							when not matched then
								insert ({{ insert_columns_str }}, src_load_ts, batch_id, etl_load_ts, etl_updt_ts)
								values (src.{{ select_columns_str }}, src.src_load_ts, src.batch_id, src.etl_load_ts, src.etl_updt_ts);
							commit;
						{%- endset %}
						{% do run_query(merge_sql) %}

						{% break %}
					{% else %}
						{% do log("The table is currently undergoing replication", info = True) %}
					{% endif %}					
					{% break %}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {% endif %}
{% endmacro %}