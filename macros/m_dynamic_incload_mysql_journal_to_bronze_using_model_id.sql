{% macro m_dynamic_incload_mysql_journal_to_bronze_using_model_id(p_pipeline_name, p_model_grp, p_model_id, p_inc_load_ts, p_batch_id) %}
  {% do log("Initiating the load from MySQL Journal to Bronze for the model: " ~ p_model_id, info=True) %}

  {% set v_pipeline_name = p_pipeline_name %}
  {% set v_model_grp = p_model_grp %}
  {% set v_model_id = p_model_id %}
  {% set v_inc_load_ts = p_inc_load_ts %}
  {% set v_batch_id = p_batch_id %}

  {% set v_src_tbl = '' %}
  {% set v_tgt_tbl = '' %}
  {% set v_load_type = '' %}
  {% set v_inc_load_ts_column = '' %}
  {% set v_primary_key_columns = [] %}

  {# Fetch the source yaml model attributes #}
  {% for source in graph.sources.values() %}
    {% if source.source_name == v_model_grp and source.meta.tgt_tbl == v_model_id %}
      {% set v_model_id_short = source.name %}
      {% set v_src_tbl = source.meta.src_tbl %}
      {% set v_tgt_tbl = source.meta.tgt_tbl %}
      {% set v_load_type = source.meta.load_type %}
      {% set v_inc_load_ts_column = source.loaded_at_field %}
      {% do log("Setting src_tbl: " ~ v_src_tbl ~ ", model_id: " ~ v_model_id_short ~ ", tgt_tbl: " ~ v_tgt_tbl ~ ", load_type: " ~ v_load_type ~ ", inc_load_ts_column: " ~ v_inc_load_ts_column, info=True) %}

      {# Fetch column information for source and target tables for source yaml #}
      {% set v_src_db = v_src_tbl.split('.')[0] %}
      {% set v_src_schema = v_src_tbl.split('.')[1] %}
      {% set v_src_table = v_src_tbl.split('.')[2] %}
      {% set v_tgt_db = v_tgt_tbl.split('.')[0] %}
      {% set v_tgt_schema = v_tgt_tbl.split('.')[1] %}
      {% set v_tgt_table = v_tgt_tbl.split('.')[2] %}

      {% set replication_status = ftl_utils.fn_mysql_agent_check(v_model_grp, v_model_id_short) %}
      {% if replication_status == 'running' %}

        {# Step 1: Fetch latest two journal tables for this model_id #}
        {% set journal_sql %}
          select table_name
          from {{ v_src_db }}.information_schema.tables
          where lower(trim(table_schema)) = 'journals'
            and lower(table_name) not like '%snapshots%'
            and upper(regexp_substr(table_name, '^(.*)-', -1)) = upper('{{ v_src_table }}')||'_'||split_part(table_name, '_', -1)
          order by table_name desc
          limit 2
        {% endset %}

        {% call statement('get_journal_tables', fetch_result=true) %}
          {{ journal_sql }}
        {% endcall %}
        {% set journal_tbls = load_result('get_journal_tables')['data'] %}
        {% set journal_union_sql = [] %}

        {% for row in journal_tbls %}
          {% set tbl_name = row[0] %}
          {% set union_sql %}
            select
              primary_key_pk, event_type, most_significant_position, least_significant_position, seen_at, sf_metadata,
              object_delete(object_construct(*), 'PRIMARY_KEY_PK', 'EVENT_TYPE', 'MOST_SIGNIFICANT_POSITION', 'LEAST_SIGNIFICANT_POSITION', 'SEEN_AT', 'SF_METADATA')
              as attribute_value_json
            from {{ v_src_db }}."journals"."{{ tbl_name }}" as src_journal_tbl
            where primary_key_pk is not null and {{ v_inc_load_ts_column }} > '{{ v_inc_load_ts }}'
          {% endset %}
          {% do journal_union_sql.append(union_sql) %}
        {% endfor %}

        {% set final_union_query = journal_union_sql | join('\n union all \n') %}

        {% set delete_sql %}
          begin;
          delete from {{ v_tgt_tbl }}
          {% if v_load_type == 'full' %}
            where {{ v_inc_load_ts_column }} > '{{ v_inc_load_ts }}'
          {% endif %}
          ;
          commit;
        {% endset %}
        {% do run_query(delete_sql) %}

        {# The Incremental (New batch) data load is then inserted in to the Target #}
        {% set insert_sql %}
          begin;
          insert into {{ v_tgt_tbl }} (
            primary_key_pk, event_type, most_significant_position, least_significant_position,
            seen_at, sf_metadata, attribute_value_json,
            src_journal_tbl, src_load_ts, batch_id, etl_load_ts, etl_updt_ts
          )
          select
            primary_key_pk, event_type, most_significant_position, least_significant_position,
            seen_at, sf_metadata, attribute_value_json,
            '{{ v_model_id_short }}' as src_journal_tbl,
            seen_at as src_load_ts,
            '{{ v_batch_id }}'::number(38, 0) as batch_id,
            current_timestamp()::timestamp_ntz(9) as etl_load_ts,
            current_timestamp()::timestamp_ntz(9) as etl_updt_ts
          from (
            {{ final_union_query }}
          ) src;
          commit;
        {% endset %}
        {% do run_query(insert_sql) %}
      {% else %}
        {% do log("The table is currently undergoing replication", info = True) %}
      {% endif %}
      {% break %}
    {% endif %}
  {% endfor %}
{% endmacro %}
