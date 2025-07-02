{% macro m_dynamic_load_extstage_to_stage_using_model_id( p_pipeline_name, model_grp, model_id) %}
  {% do log("Initiating the load from External Stage to Bronze for the model: " ~ model_id, info=True) %}
  {% if execute %}
    {%- set tgt_tbl = '' -%}
    {%- set file_pattern = '' -%}
    {%- set ext_stage = '' -%}
    {%- set file_format = '' -%}
    {%- set stage_name_with_file = '' -%}
    {%- set file_header = '' -%}

    {# Fetch the source yaml model attributes #}
    {% for source in graph.sources.values() %}
      {%- if source.source_name == model_grp -%}
        {%- if source.meta.tgt_tbl == model_id -%}
          {%- set tgt_tbl = source.meta.tgt_tbl -%}
          {% do log("Setting tgt_tbl : " ~ tgt_tbl, info=True) %}
          {%- set file_pattern = source.meta.file_pattern -%}
          {% do log("Setting FILE Pattern : " ~ file_pattern, info=True) %}
          {%- set file_format = source.meta.file_format -%}
          {% do log("Setting FILE FORMAT : " ~ file_format, info=True) %}
          {%- set ext_stage = source.meta.ext_stage -%}
          {% do log("Setting Snowflake Ext stage : " ~ ext_stage, info=True) %}
          {%- set stage_name_with_file = ext_stage ~ file_pattern -%}
          {%- set file_header = source.meta.file_header -%}
          {% do log("Setting File header : " ~ file_header, info=True) %}

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
          {%- set columns_to_remove = ['etl_load_ts','etl_updt_ts'] -%}
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

          {# In this step based on the file_header flag, The type of copy activity is chosen and,
          which first copies the data
          load from source file to the temporary table which is created and then this is moved to main Bronze table with its respective mapping created in the column mapping. #}
          {% if file_header == 'y' %}
            {% set trunc_sql %}
              begin;
              truncate table {{ tgt_tbl }};
              commit;
            {% endset %}
            {% do run_query(trunc_sql) %}

            {% set copy_sql %}
              begin;
              copy into {{ tgt_tbl }}
              from {{ stage_name_with_file }}
              file_format = (format_name => {{ file_format }})
              match_by_column_name = case_insensitive
              on_error = 'CONTINUE';
              commit;
            {% endset %}
            {% do run_query(copy_sql) %}
          {% else %}
            {% do log("Format type not specified, Please check and re assign the format", info=True) %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
