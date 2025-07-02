{% macro m_dynamic_backfill_mysql_bronze_to_silver_using_model_id(p_pipeline_name, p_model_grp, p_model_id, p_inc_load_ts, p_batch_id) %}
  {% do log("Initiating the load from MySQL Stage to Silver for the model: " ~ p_model_id, info=True) %}
  {% set v_pipeline_name = p_pipeline_name %}
  {% set v_model_grp = p_model_grp %}
  {% set v_model_id = p_model_id %}
  {% set v_inc_load_ts = p_inc_load_ts %}
  {% set v_batch_id = p_batch_id %}

  {% if execute %}
    {%- set v_src_tbl = '' -%}
    {%- set v_tgt_tbl = '' -%}
    {%- set v_load_type = '' -%}
    {%- set v_inc_load_ts_column = '' -%}
    {%- set v_primary_key_columns = [] -%}

    {# Fetch the source yaml model attributes #}
    {% for src in graph.sources.values() %}
      {% if src.source_name == v_model_grp %}
        {%- if src.meta.tgt_tbl == v_model_id -%}
          {%- set v_src_tbl = src.meta.archive_tbl -%}
          {%- set v_tgt_tbl = src.meta.tgt_tbl -%}
          {%- set v_load_type = src.meta.load_type -%}
          {%- set v_inc_load_ts_column = src.meta.deduped_at_field -%}
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
            select replace(lower(column_name), 'payload__', '') as column_name
            from {{ v_src_db }}.information_schema.columns
            where lower(table_schema) = replace(lower('{{ v_src_schema }}'),'"','')
              and lower(table_name) = lower('{{ v_src_table }}')
              and lower(column_name) not in ('primary_key_pk','event_type','most_significant_position','least_significant_position','seen_at','_metadata','src_journal_tbl')
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
              and lower(column_name) not in ('_snowflake_inserted_at','_snowflake_updated_at','_snowflake_deleted')
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

          {# Columns exclusion for the schema comparison Source to Target, and filter out these columns #}
          {% set columns_to_remove = ['src_load_ts','batch_id','etl_load_ts','etl_updt_ts'] %}
          {% set filtered_tgt_columns_list = [] %}
          {% for col in tgt_columns_list %}
            {% if col not in columns_to_remove %}
              {% set _ = filtered_tgt_columns_list.append(col) %}
            {% endif %}
          {% endfor %}
          {% set tgt_columns_list = filtered_tgt_columns_list %}

          {% set filtered_src_columns_list = [] %}
          {% for col in src_columns_list %}
            {% if col not in columns_to_remove %}
              {% set _ = filtered_src_columns_list.append(col) %}
            {% endif %}
          {% endfor %}
          {% set src_columns_list = filtered_src_columns_list %}

          {% set replication_status = ftl_utils.fn_mysql_agent_check(v_model_grp, v_src_table) %}
          {% if replication_status != 'running' %}

            {# Schema comparison from Source to Target #}
            {% if src_columns_list | sort == tgt_columns_list | sort %}
              {% do log("Source and target schemas match after removing specified columns. Proceeding with data transfer.", info=True) %}

              {# SQL preparation of Source & Target columns #}
              {% set pk_columns_str = v_primary_key_columns | join(', ') %}
              {% set insert_columns_str = tgt_columns_list | join(', ') %}
              {% set select_map_columns = [] %}
              {% for col in tgt_columns_list %}
                {% set _ = select_map_columns.append('src.' ~ col ~ ' as ' ~ col) %}
              {% endfor %}
              {% set select_map_columns_str = select_map_columns | join(', ') %}

              {% set update_columns_list = [] %}
              {% for col in tgt_columns_list %}
                {% if col not in v_primary_key_columns %}
                  {% set _ = update_columns_list.append(col) %}
                {% endif %}
              {% endfor %}

              {% set update_conditions = [] %}
              {% for col in update_columns_list %}
                {% set update_condition = "tgt." ~ col ~ " != src." ~ col %}
                {% do update_conditions.append(update_condition) %}
              {% endfor %}
              {% set update_columns_str = update_conditions | join(' or ') %}

              {# Merge SQL Logic #}
              {% set merge_sql %}
              merge into {{ v_tgt_tbl }} as tgt
              using (
                with src_tbl as (
                  select *, row_number() over (
                    partition by src.primary_key_pk 
                    order by src.least_significant_position desc, src.seen_at desc
                  ) as rnk
                  from {{ v_src_tbl }} src
                ),
                src_final as (
                  select
                    {{ select_map_columns_str }},
                    src.seen_at as _snowflake_inserted_at,
                    src.seen_at as _snowflake_updated_at,
                    null as _snowflake_deleted,
                    src.src_load_ts as src_load_ts,
                    '{{ v_batch_id }}'::number(38, 0) as batch_id,
                    current_timestamp()::timestamp_ntz(9) as etl_load_ts,
                    current_timestamp()::timestamp_ntz(9) as etl_updt_ts
                  from src_tbl src
                  where rnk = 1 and lower(event_type) != 'incrementaldeletes'
                )
                select * from src_final src
                where not exists (
                  select 1 from {{ v_tgt_tbl }} tgt
                  where src.pk=tgt.pk and src.version=tgt.version
                )
              ) as src
              on ({{ pk_join_condition }})
              when matched then update set
                {{ update_columns_str }},
                tgt._snowflake_inserted_at=src._snowflake_inserted_at,
                tgt._snowflake_updated_at=src._snowflake_updated_at,
                tgt._snowflake_deleted=src._snowflake_deleted,
                tgt.src_load_ts=src.src_load_ts,
                tgt.batch_id = src.batch_id,
                tgt.etl_load_ts = src.etl_load_ts,
                tgt.etl_updt_ts = src.etl_updt_ts
              when not matched then insert (
                {{ insert_columns_str }}, _snowflake_inserted_at, _snowflake_updated_at, _snowflake_deleted,
                src_load_ts, batch_id, etl_load_ts, etl_updt_ts
              )
              values (
                src.{{ insert_columns_str }}, src._snowflake_inserted_at, src._snowflake_updated_at, 
                src._snowflake_deleted, src.src_load_ts, src.batch_id, src.etl_load_ts, src.etl_updt_ts
              );
              commit;
              {% endset %}

              {% do run_query(merge_sql) %}
            {% else %}
              {{ exceptions.raise_compiler_error("Source and target schemas do not match for the model: " ~ v_tgt_tbl ~ ". Aborting the data transfer.") }}
            {% endif %}
          {% else %}
            {% do log("The table is currently undergoing replication", info=True) %}
          {% endif %}
          {% break %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
