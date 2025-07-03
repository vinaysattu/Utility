{% macro m_ddl_generator(p_model_grp, p_model_id='ALL', p_model_type='table', p_tgt_layer='silver') %}
    {%- set ddl_statements = [] -%}
    {%- set v_model_grp = p_model_grp -%}
    {%- set v_model_id = p_model_id -%}
    {%- set v_model_type = p_model_type -%}
    {%- set v_tgt_layer = p_tgt_layer -%}

    {% if execute %}
        {%- for source in graph.sources.values() %}
            {%- if source.source_name == v_model_grp and (v_model_id == 'ALL' or source.name == v_model_id) %}
                {% if v_model_type == 'view' and v_tgt_layer == 'bronze' %}
                    {%- set v_schema_ref_tbl = source.meta.get('tgt_tbl', '') -%}
                    {%- set v_tgt_tbl = source.meta.get('src_archive_tbl', '') -%}
                    {%- set v_src_tbl = v_tgt_tbl[:-2] if v_tgt_tbl.endswith('_v') or v_tgt_tbl.endswith('_V') else v_tgt_tbl -%}
                {% else %}
                    {%- set v_src_obj = source.meta.get('src_tbl', '') -%}
                    {%- set v_tgt_tbl = source.meta.get('tgt_tbl', '') -%}
                    {%- set v_src_archive_tbl = source.meta.get('src_archive_tbl', '') -%}
                    {%- set v_tgt_vw = v_tgt_tbl[:-2] ~ '_v' if v_tgt_tbl.endswith('_t') or v_tgt_tbl.endswith('_T') else v_tgt_tbl ~ '_v' -%}
                    {%- set v_src_tbl = v_src_obj[:-2] if v_src_obj.endswith('_v') or v_src_obj.endswith('_V') else v_src_obj -%}
                {% endif %}

                {% do log("Processing model: " ~ source.source_name ~ "." ~ source.name, info=true) %}

                {% if v_src_tbl and v_tgt_tbl and v_tgt_layer %}
                    {% if v_model_type == 'table' %}
                        {% if v_tgt_layer == 'bronze' %}
						
							{% set v_src_db = v_src_tbl.split('.')[0] %}
							{% set v_src_schema = v_src_tbl.split('.')[1] %}
							{% set v_src_table = v_src_tbl.split('.')[2] %}

							{% set actual_src_tbl_query %}
								select table_name
								from {{ v_src_db }}.information_schema.tables
								where upper(trim(table_schema)) = 'JOURNALS'
								and upper(trim(table_name)) not like '%SNAPSHOT%'
								and upper(regexp_substr(table_name,upper('{{ v_src_table }}'))) =upper('{{ v_src_table }}')
								order by created desc
								limit 1
							{% endset %}
							{% call statement('actual_src_tbl', fetch_result=True) %}
								{{ actual_src_tbl_query }}
							{% endcall %}
			
							{% set actual_src_result = load_result('actual_src_tbl') %}
							{% if actual_src_result['data'] and actual_src_result['data'] | length > 0 %}
								{% set v_actual_src_tbl = v_src_db ~ '.JOURNALS."' ~ actual_src_result['data'][0][0] ~ '"' %}
							{% else %}
								{% set v_actual_src_tbl = none %}
							{% endif %}
				
                            {% if v_actual_src_tbl %}
                                {% set columns_query %} describe table {{ v_actual_src_tbl }} {% endset %}
                            {% else %}
                                {% do log("Skipping model due to missing actual source table.", info=true) %}
                                {% set columns_query = "" %}
                            {% endif %}
                        {% elif v_tgt_layer == 'silver' %}
                            {% set columns_query %} describe table {{ v_src_tbl }} {% endset %}
                        {% else %}
                            {% do log("Unknown target layer: " ~ v_tgt_layer, info=true) %}
                            {% set columns_query = "" %}
                        {% endif %}
                    {% elif v_model_type == 'view' %}
                        {% if v_tgt_layer == 'bronze' %}
                            {% set columns_query %} describe table {{ v_schema_ref_tbl }} {% endset %}
                        {% elif v_tgt_layer == 'silver' %}
                            {% set columns_query %} describe table {{ v_tgt_tbl }} {% endset %}
                        {% elif v_tgt_layer == 'stage' %}
                            {% set columns_query %} describe table {{ v_src_tbl }} {% endset %}
                        {% else %}
                            {% do log("Unknown target layer: " ~ v_tgt_layer, info=true) %}
                            {% set columns_query = "" %}
                        {% endif %}
                    {% else %}
                        {% do log("Unknown model type: " ~ v_model_type, info=true) %}
                        {% set columns_query = "" %}
                    {% endif %}

                    {% if columns_query != "" %}
                        {% set columns_result = run_query(columns_query) %}
                        {% set columns = [] %}
                        {% set data_types = {} %}

                        {% for row in columns_result %}
                            {% set _ = columns.append(row['name'].lower()) %}
                            {% set cleaned_data_type = row['type'].lower().replace('varchar(16777216)', 'varchar') %}
                            {% set _ = data_types.update({row['name'].lower(): cleaned_data_type}) %}
                        {% endfor %}

                        {% if v_model_type == 'table' %}
                            {% if v_tgt_layer == 'bronze' %}
                                {% set additional_cols = [
                                    "primary_key__pk number(38,0)",
                                    "event_type varchar",
                                    "most_significant_position number(38,0)",
                                    "least_significant_position number(38,0)",
                                    "seen_at timestamp_ntz(9)",
                                    "sf_metadata variant",
                                    "attribute_value_json variant",
                                    "src_journal_tbl varchar",
                                    "src_load_ts timestamp_ntz(9)",
                                    "batch_id number(38,0)",
                                    "etl_load_ts timestamp_ntz(9) default current_timestamp()",
                                    "etl_updt_ts timestamp_ntz(9) default current_timestamp()",
                                    "primary key (primary_key__pk)"
                                ] %}
                                {% set ddl_statement = "create or replace table " ~ v_tgt_tbl | lower ~ " (\n    " ~ additional_cols | join(",\n    ") ~ "\n);" %}
                            {% elif v_tgt_layer == 'silver' %}
                                {% set pk_columns = [] %}
                                {% set sk_columns = [] %}
                                {% set id_columns = [] %}
                                {% set is_columns = [] %}
                                {% set attr_columns = [] %}
                                {% set metric_columns = [] %}
                                {% set date_time_columns = [] %}
                                {% set meta_columns = [] %}
                                {% set aux_columns = [] %}
                                {% for column in columns %}
                                    {% set column_lower = column.lower() %}
                                    {% if column_lower == 'pk' %}
                                        {% set _ = pk_columns.append(column) %}
                                    {% elif column_lower.endswith('_pk') %}
                                        {% set _ = sk_columns.append(column) %}
                                    {% elif column_lower.endswith('_id') %}
                                        {% set _ = id_columns.append(column) %}
                                    {% elif column_lower.startswith('is_') %}
                                        {% set _ = is_columns.append(column) %}
                                    {% elif column_lower.startswith('_snowflake') %}
                                        {% set _ = meta_columns.append(column) %}
                                    {% elif column_lower.endswith('_metric') %}
                                        {% set _ = metric_columns.append(column) %}
                                    {% elif column_lower.endswith('_date') or column_lower.endswith('_timestamp') %}
                                        {% set _ = date_time_columns.append(column) %}
                                    {% else %}
                                        {% set _ = aux_columns.append(column) %}
                                    {% endif %}
                                {% endfor %}
                                {% set ordered_columns = pk_columns + sk_columns + id_columns + is_columns + attr_columns + aux_columns + metric_columns + date_time_columns + meta_columns %}
                                {% set additional_cols = "    src_load_ts timestamp_ntz(9),\n    batch_id number(38,0),\n    etl_load_ts timestamp_ntz(9) default current_timestamp(),\n    etl_updt_ts timestamp_ntz(9) default current_timestamp()" %}
                                {% set formatted_columns = [] %}
                                {% for col in ordered_columns %}
                                    {% set _ = formatted_columns.append('    ' ~ col ~ ' ' ~ data_types[col]) %}
                                {% endfor %}
                                {% set ddl_statement = "create or replace table " ~ v_tgt_tbl.lower() ~ " (\n" ~ formatted_columns | join(",\n") ~ ",\n" ~ additional_cols ~ ",\n    primary key (" ~ pk_columns | join(", ") ~ ")\n );" %}
                            {% endif %}
                        {% elif v_model_type == 'view' %}
                            {% if v_tgt_layer == 'bronze' %}
                                {% set json_parse_columns = [] %}
                                {% set metadata_columns = ['primary_key__pk', 'event_type', 'most_significant_position', 'least_significant_position', 'seen_at', 'sf_metadata', 'src_journal_tbl', 'src_load_ts', 'batch_id', 'etl_load_ts', 'etl_updt_ts'] %}
                                {% set attribute_exclude_columns = ['_snowflake_inserted_at','_snowflake_updated_at','_snowflake_deleted'] %}
                                {% for col in columns %}
                                    {% if col not in metadata_columns and col not in attribute_exclude_columns %}
                                        {% set _ = json_parse_columns.append("    attribute_value_json:PAYLOAD__" ~ col.upper() ~ "::" ~ data_types[col] ~ " as " ~ col) %}
                                    {% endif %}
                                {% endfor %}
                                {% set all_selects = metadata_columns | list %}
                                {% set all_selects_formatted = [] %}
                                {% for col in all_selects %}
                                    {% set _ = all_selects_formatted.append("    " ~ col) %}
                                {% endfor %}
                                {% set all_selects_formatted = json_parse_columns + all_selects_formatted %}
                                {% set ddl_statement = "create or replace view " ~ v_tgt_tbl | lower ~ " as\nselect\n" ~ all_selects_formatted | join(",\n") ~ "\nfrom\n    " ~ v_src_tbl | lower ~ ";" %}
                            {% elif v_tgt_layer == 'silver' or v_tgt_layer == 'stage' %}
                                {% set formatted_columns = [] %}
                                {% for col in columns %}
                                    {% set _ = formatted_columns.append('    ' ~ col) %}
                                {% endfor %}
                                {% set ddl_statement = "create or replace view " ~ (v_tgt_vw if v_tgt_layer != 'stage' else v_src_obj) | lower ~ " as\nselect\n" ~ formatted_columns | join(",\n") ~ "\nfrom\n    " ~ (v_tgt_tbl if v_tgt_layer != 'stage' else v_src_tbl) | lower ~ ";" %}
                            {% else %}
                                {% set ddl_statement = "Unknown target layer: " ~ v_tgt_layer %}
                            {% endif %}
                        {% endif %}

                        {% do ddl_statements.append(ddl_statement) %}
                    {% else %}
                        {% do log("Skipping model due to empty or missing columns query", info=true) %}
                    {% endif %}
                {% else %}
                    {% do log("Skipping model due to missing metadata for " ~ source.source_name ~ "." ~ source.name, info=true) %}
                {% endif %}
            {%- endif %}
        {%- endfor %}
    {% endif %}

    {% set final_output = ddl_statements | join("\n\n") %}
    {% do log("Generated DDLs: \n" ~ final_output, info=true) %}
    {{ return(final_output) }}
{% endmacro %}
