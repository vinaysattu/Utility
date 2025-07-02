{% macro m_dynamic_load_extstage_to_bronze_using_model_id( p_pipeline_name, model_grp, model_id, v_batch_id) %}
  {% do log("Initiating the load from External Stage to Bronze for the model: " ~ model_id, info=True) %}
  {% if execute %}
    {%- set tgt_tbl = '' -%}
    {%- set file_pattern = '' -%}
    {%- set ext_stage = '' -%}
    {%- set file_format = '' -%}
    {%- set file_header = '' -%}
    {%- set stage_name_with_file = '' -%}
    {%- set file_archive_flg = '' -%}
    {%- set archive_path = '' -%}
    {%- set stage_name_with_archive = '' -%}
    {%- set archive_retention_days = '' -%}

    {# Fetch the source yaml model attributes #}
    {% for source in graph.sources.values() %}
      {%- if source.source_name == model_grp -%}
        {%- if source.meta.tgt_tbl == model_id -%}
          {%- set tgt_tbl = source.meta.tgt_tbl -%}
          {% do log("Setting tgt_tbl : " ~ tgt_tbl, info=True) %}
          {%- set file_pattern = source.meta.file_pattern -%}
          {% do log("Setting FILE Pattern: " ~ file_pattern, info=True) %}
          {%- set file_format = source.meta.file_format -%}
          {% do log("Setting FILE FORMAT: " ~ file_format, info=True) %}
          {%- set ext_stage = source.meta.ext_stage -%}
          {% do log("Setting SnowFlake Ext stage: " ~ ext_stage, info=True) %}
          {%- set stage_name_with_file = ext_stage ~ file_pattern -%}
          {%- set file_header = source.meta.file_header -%}
          {% do log("Setting File header : " ~ file_header, info=True) %}
          {%- set column_mapping = source.meta.column_mapping if 'column_mappings' in source.meta else {} -%}
          {% do log("Setting Column Mapping : " ~ column_mapping, info=True) %}
          {%- set truncate_flg = source.meta.truncate_flg -%}
          {%- set file_archive_flg = source.meta.file_archive_flg -%}
          {% do log("Setting File Archive Flag : " ~ file_archive_flg, info=True) %}
          {%- set archive_path = source.meta.archive_path -%}
          {%- set stage_name_with_archive = ext_stage ~ archive_path -%}
          {%- set archive_retention_days = source.meta.archive_retention_days -%}

          {# Fetch column information for target tables for source yaml #}
          {% set tgt_db = tgt_tbl.split('.')[0] %}
          {% set tgt_schema = tgt_tbl.split('.')[1] %}
          {% set tgt_table = tgt_tbl.split('.')[2] %}

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

          {# Columns exclusion for the schema comparison Source to Target, and filter out these columns, out of the comparison #}
          {%- set columns_to_remove = ['batch_id', 'src_load_ts', 'etl_load_ts', 'etl_updt_ts'] -%}
          {%- set filtered_tgt_columns_list = [] -%}
          {%- for col in tgt_columns_list -%}
            {%- if col not in columns_to_remove -%}
              {%- set _ = filtered_tgt_columns_list.append(col) -%}
            {%- endif -%}
          {%- endfor -%}
          {%- set tgt_columns_list = filtered_tgt_columns_list -%}

          {# SQL preparation of Source & Target columns #}
          {% set insert_columns_str = tgt_columns_list | join(', ') %}
          {% set select_columns_str = tgt_columns_list | join(', src.') %}
          {% set false_insert_columns_list = [] %}
          {% set false_select_columns_list = [] %}
          {% for key, value in column_mapping.items() %}
            {% set _ = false_insert_columns_list.append(value) %}
            {% set _ = false_select_columns_list.append('src.' ~ key ~ " as " ~ value) %}
          {% endfor %}
          {% set false_insert_columns_str = false_insert_columns_list | join(', ') %}
          {% set false_select_columns_str = false_select_columns_list | join(', ') %}

          {# This step creates a temporary table , with the source file schema #}
          {% set create_sql %}
            begin;
            create or replace temporary table {{ tgt_tbl }}_tmp
            using template (
              select array_agg(object_construct(*))
              within group (order by order_id)
              from table(
                infer_schema(
                  location => '{{ stage_name_with_file }}',
                  file_format => '{{ file_format }}'
                )
              )
            );
            commit;
          {% endset %}
          {% do run_query(create_sql) %}

          {# In this step based on the file_header flag, The type of copy activity is chosen and, 
          which first copies the data load
          from source file to the temporary table which is created and thn this is moved to main Bronze table with its respective mapping created in the column mapping. #}
          {% if file_header == 'y' %}
            {% set copy_sql %}
              begin;
              copy into {{ tgt_tbl }}_tmp
              from {{ stage_name_with_file }}
              file_format = (format_name => {{ file_format }})
              match_by_column_name = case_insensitive;
              commit;
            {% endset %}
            {% do run_query(copy_sql) %}

            {% if truncate_flg == 'y' %}
              {% set trunc_sql %}
                begin;
                truncate table {{ tgt_tbl }};
                commit;
              {% endset %}
              {% do run_query(trunc_sql) %}
            {% endif %}

            {% set insert_sql %}
              begin;
              insert into {{ tgt_tbl }} (
                {{ insert_columns_str }},batch_id,src_load_ts,etl_load_ts,etl_updt_ts)
              select
                src.{{ select_columns_str }},
                '{{ v_batch_id }}'::number(38, 0) as batch_id,
                current_timestamp()::timestamp_ntz(9) as src_load_ts,
                current_timestamp()::timestamp_ntz(9) as etl_load_ts,
                current_timestamp()::timestamp_ntz(9) as etl_updt_ts
              from {{ tgt_tbl }}_tmp src;
              commit;
            {% endset %}
            {% do run_query(insert_sql) %}

          {% elif file_header == 'n' %}
            {% set copy_sql %}
              begin;
              copy into {{ tgt_tbl }}_tmp
              from {{ stage_name_with_file }}
              file_format = (format_name => {{ file_format }});
              commit;
            {% endset %}
            {% do run_query(copy_sql) %}

            {% if truncate_flg == 'y' %}
              {% set trunc_sql %}
                begin;
                truncate table {{ tgt_tbl }};
                commit;
              {% endset %}
              {% do run_query(trunc_sql) %}
            {% endif %}

            {% set insert_sql %}
              begin;
              insert into {{ tgt_tbl }}
              ({{ false_insert_columns_str }},batch_id,src_load_ts,etl_load_ts,etl_updt_ts)
              select
                {{ false_select_columns_str }},
                '{{ v_batch_id }}'::number(38, 0) as batch_id,
                current_timestamp()::timestamp_ntz(9) as src_load_ts,
                current_timestamp()::timestamp_ntz(9) as etl_load_ts,
                current_timestamp()::timestamp_ntz(9) as etl_updt_ts
              from {{ tgt_tbl }}_tmp src;
              commit;
            {% endset %}
            {% do run_query(insert_sql) %}

          {% endif %}

          {% if file_archive_flg == 'y' %}
            {# Copying Ingested files to archive Path #}
            {% set copy_file_to_archive_sql %}
              copy files into {{ stage_name_with_archive }}
              from {{ stage_name_with_file }};
            {% endset %}
            {% do run_query(copy_file_to_archive_sql) %}

            {# Removing Files Older than Archive Retention days in the archive Path #}
            {% set Remove_older_file_sql %}
              begin;
              call {{ tgt_db }}.{{ tgt_schema }}.remove_stage_files('{{ stage_name_with_archive }}', {{ archive_retention_days }}, false);
              end;
            {% endset %}
            {% do run_query(Remove_older_file_sql) %}
          {% endif %}
        {% else %}
          {% do log("Format type not specified, Please check and re assign the format", info=True) %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
