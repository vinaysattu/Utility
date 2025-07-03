{% macro m_dynamic_delinsload_bronze_to_silver_using_model_id( p_pipeline_name, model_grp, model_id, v_batch_id ) %}
    {% do log("Initiating the load(Delete & Insert) from Bronze to Silver for the model: " ~ model_id, info=True) %}
    {% if execute %}
        {%- set src_tbl = '' -%}
        {%- set tgt_tbl = '' -%}
        {%- set load_type = '' -%}
        {%- set primary_key_columns = [] -%}  
		{# Fetch the source yaml model attibutes #}
        {% for source in graph.sources.values() %}
            {%- if source.source_name == model_grp -%}
                {%- if source.meta.tgt_tbl == model_id -%}
                    {%- set src_tbl = source.meta.src_tbl -%}
                    {%- set tgt_tbl = source.meta.tgt_tbl -%}
                    {%- set load_type = source.meta.load_type -%}
                    {% do log("Setting src_tbl: " ~ src_tbl ~ ", tgt_tbl: " ~ tgt_tbl ~ ", load_type: " ~ load_type, info=True) %}

                    {# Fetch column information for source and target tables for source yaml #}
                    {% set src_db = src_tbl.split('.')[0] %}
                    {% set src_schema = src_tbl.split('.')[1] %}
                    {% set src_table = src_tbl.split('.')[2] %}
                    {% set tgt_db = tgt_tbl.split('.')[0] %}
                    {% set tgt_schema = tgt_tbl.split('.')[1] %}
                    {% set tgt_table = tgt_tbl.split('.')[2] %}

                    {# Fetch source columns using the source yaml attribute for source table #}
                    {% call statement('fetch_src_columns', fetch_result=true) %}
                        select lower(column_name) as column_name
                        from {{ src_db }}.information_schema.columns
                        where lower(table_schema) = lower('{{ src_schema }}')
                          and lower(table_name) = lower('{{ src_table }}')
                    {% endcall %}
                    {% set src_columns_result = load_result('fetch_src_columns') %}
                    {% set src_columns_list = [] %}
                    {% for col in src_columns_result['data'] %}
                        {% set _ = src_columns_list.append(col[0]) %}
                    {% endfor %}

                    {# Fetch target columns using the source yaml attribute for target table #}
                    {% call statement('fetch_tgt_columns', fetch_result=true) %}
                        select lower(column_name) as column_name
                        from {{ tgt_db }}.information_schema.columns
                        where lower(table_schema) = lower('{{ tgt_schema }}')
                          and lower(table_name) = lower('{{ tgt_table }}')
                    {% endcall %}
                    {% set tgt_columns_result = load_result('fetch_tgt_columns') %}
                    {% set tgt_columns_list = [] %}
                    {% for col in tgt_columns_result['data'] %}
                        {% set _ = tgt_columns_list.append(col[0]) %}
                    {% endfor %}

                    {# Fetch target table's primary keys #}
                    {% call statement('fetch_primary_keys', fetch_result=true) %}
                        show primary keys in table {{ tgt_db }}.{{ tgt_schema }}.{{ tgt_table }};
                    {% endcall %}
                    {% set pk_columns_result = load_result('fetch_primary_keys') %}
                    {% set primary_key_columns = [] %}
                    {% for col in pk_columns_result['data'] %}
                        {% set _ = primary_key_columns.append(col[4] | lower) %}  -- Column 3 contains the primary key column name
                    {% endfor %}
                    {% set conditions = [] %}
                    {% for key in primary_key_columns %}
                        {% set condition = "src." ~ key ~ " = tgt." ~ key %}
                        {% do conditions.append(condition) %}
                    {% endfor %}
                    {% set pk_join_condition = conditions | join(' and ') %}

                    {# Columns exclusion for the schema comaparsion Soruce to Target, and filter out these columns, out of the comaprison #}
                    {%- set columns_to_remove = ['batch_id', 'etl_load_ts', 'etl_updt_ts'] -%}
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

                    {# Schema comparison from Source to Target, The load is proceeded, only when the comparision is a matched case #}
                    {% if src_columns_list | sort == tgt_columns_list | sort %}
                        {% do log("Source and target schemas match after removing specified columns. Proceeding with data transfer.", info=True) %}
                        
						{# SQL preperation of Source & Target coulmns #}
						{% set pk_columns_str = primary_key_columns | join(', ') %}
                        {% set insert_columns_str = tgt_columns_list | join(', ') %}
                        {% set select_columns_str = tgt_columns_list | join(', src.') %}

                        {# First, delete the existing records in target where primary keys match for the batch #}
						{% set delete_sql -%}
                            begin;
							delete from {{ tgt_tbl }} tgt
							where exists (
								select 1
								from {{ src_tbl }} src
								where {{ pk_join_condition }} 
                                and src.etl_load_ts > (select COALESCE(MAX(etl_updt_ts), '1900-01-01') from {{ tgt_tbl }} )
							);
							commit;
                        {%- endset %}
                        {% do run_query(delete_sql) %}

                        {# Then, insert the new records with batch_id #}
						{% set insert_sql -%}
                            begin;
							insert into {{ tgt_tbl }} ({{ insert_columns_str }}, batch_id, etl_load_ts, etl_updt_ts)
							select {{ select_columns_str }},
								{{ v_batch_id }}::number(38, 0) as batch_id,
								current_timestamp()::timestamp_ntz(9) as etl_load_ts,
								current_timestamp()::timestamp_ntz(9) as etl_updt_ts
							from {{ src_tbl }} src where src.etl_load_ts > (select COALESCE(MAX(etl_updt_ts), '1900-01-01') from {{ tgt_tbl }} );
							commit;
                        {%- endset %}
                        {% do run_query(insert_sql) %}

                    {% else %}
                        {{ exceptions.raise_compiler_error("Source and target schemas do not match for the model: " ~ tgt_tbl ~ ". Aborting the data transfer." ) }}
                    {% endif %}
                    {% break %}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {% endif %}
{% endmacro %}