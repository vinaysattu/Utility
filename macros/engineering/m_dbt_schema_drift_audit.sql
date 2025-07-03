{% macro m_capture_schema_drift_for_model_id(p_pipeline_name, p_model_grp, p_model_id, p_batch_id) %}
    {% do log("Checking schema drift for model: " ~ p_model_id, info=True) %}
    {% if execute %}
        {% set v_pipeline_name = p_pipeline_name %}
        {% set v_model_grp = p_model_grp %}
        {% set v_model_id = p_model_id %}
        {% set v_batch_id = p_batch_id %}

        {%- set v_src_tbl = '' -%}
        {%- set v_tgt_tbl = '' -%}

        {% for src in graph.sources.values() %}
            {% if src.source_name == v_model_grp and src.meta.tgt_tbl == v_model_id %}
                {% set v_src_tbl = src.meta.src_tbl %}
                {% set v_tgt_tbl = src.meta.tgt_tbl %}

                {% set v_src_db, v_src_schema, v_src_table = v_src_tbl.split('.') %}
                {% set v_tgt_db, v_tgt_schema, v_tgt_table = v_tgt_tbl.split('.') %}

                {% set exclude_columns = ["'src_load_ts'", "'batch_id'", "'etl_load_ts'", "'etl_updt_ts'"] %}

                {% set delete_sql %}
                    DELETE FROM {{ source('dbt_audit', 'dbt_schema_drift_audit_t') }} 
					WHERE model_id = '{{ v_model_id }}'
                        AND model_grp = '{{ v_model_grp }}'
                        AND pipeline_name = '{{ v_pipeline_name }}'
						AND batch_id = '{{ v_batch_id }}';
                {% endset %}

                {% do run_query(delete_sql) %}

                {% set insert_sql %}
                    INSERT INTO {{ source('dbt_audit', 'dbt_schema_drift_audit_t') }} (
                        model_id, model_grp, pipeline_name, source_table, target_table,
                        column_name, source_column_type, target_column_type,
                        diff_type, batch_id, etl_load_ts, etl_updt_ts
                    )
                    WITH
                    src_cols AS (
                        SELECT LOWER(column_name) AS column_name, LOWER(data_type) AS data_type
                        FROM {{ v_src_db }}.INFORMATION_SCHEMA.COLUMNS
                        WHERE LOWER(table_schema) = replace(LOWER('{{ v_src_schema }}'),'"','')
                          AND LOWER(table_name) = replace(LOWER('{{ v_src_table }}'),'"','')
                          AND LOWER(column_name) NOT IN ({{ exclude_columns | join(', ') }})
                    ),
                    tgt_cols AS (
                        SELECT LOWER(column_name) AS column_name, LOWER(data_type) AS data_type
                        FROM {{ v_tgt_db }}.INFORMATION_SCHEMA.COLUMNS
                        WHERE LOWER(table_schema) = LOWER('{{ v_tgt_schema }}')
                          AND LOWER(table_name) = LOWER('{{ v_tgt_table }}')
                          AND LOWER(column_name) NOT IN ({{ exclude_columns | join(', ') }})
                    ),
                    mismatched_types AS (
                        SELECT
                            '{{ v_model_id }}' AS model_id,
                            '{{ v_model_grp }}' AS model_grp,
                            '{{ v_pipeline_name }}' AS pipeline_name,
                            '{{ v_src_tbl }}' AS source_table,
                            '{{ v_tgt_tbl }}' AS target_table,
                            src.column_name,
                            src.data_type AS source_column_type,
                            tgt.data_type AS target_column_type,
                            'TYPE_MISMATCH' AS diff_type
                        FROM src_cols src
                        JOIN tgt_cols tgt ON src.column_name = tgt.column_name
                        WHERE src.data_type != tgt.data_type
                    ),
                    missing_in_target AS (
                        SELECT
                            '{{ v_model_id }}' AS model_id,
                            '{{ v_model_grp }}' AS model_grp,
                            '{{ v_pipeline_name }}' AS pipeline_name,
                            '{{ v_src_tbl }}' AS source_table,
                            '{{ v_tgt_tbl }}' AS target_table,
                            src.column_name,
                            src.data_type AS source_column_type,
                            NULL AS target_column_type,
                            'MISSING_IN_TARGET' AS diff_type
                        FROM src_cols src
                        LEFT JOIN tgt_cols tgt ON src.column_name = tgt.column_name
                        WHERE tgt.column_name IS NULL
                    ),
                    missing_in_source AS (
                        SELECT
                            '{{ v_model_id }}' AS model_id,
                            '{{ v_model_grp }}' AS model_grp,
                            '{{ v_pipeline_name }}' AS pipeline_name,
                            '{{ v_src_tbl }}' AS source_table,
                            '{{ v_tgt_tbl }}' AS target_table,
                            tgt.column_name,
                            NULL AS source_column_type,
                            tgt.data_type AS target_column_type,
                            'MISSING_IN_SOURCE' AS diff_type
                        FROM tgt_cols tgt
                        LEFT JOIN src_cols src ON tgt.column_name = src.column_name
                        WHERE src.column_name IS NULL
                    )
                    SELECT *, '{{ v_batch_id }}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    FROM mismatched_types
                    UNION ALL
                    SELECT *, '{{ v_batch_id }}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    FROM missing_in_target
                    UNION ALL
                    SELECT *, '{{ v_batch_id }}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    FROM missing_in_source;
                {% endset %}

                {% do run_query(insert_sql) %}
                {% do log("✅ Schema drift check complete for model_id: " ~ v_model_id, info=True) %}
                {% break %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
