{% macro default__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {# Retrieve columns to exclude from config #}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns', []) | map('upper') | list -%}
    {# Explicitly filter out excluded columns for update_columns #}
    {%- set update_columns = dest_columns | rejectattr("name", "in", merge_exclude_columns) | map(attribute="quoted") | list -%}
    
    {%- set sql_header = config.get('sql_header', none) -%}
    
    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    nullif(DBT_INTERNAL_SOURCE.{{ key }}, '') = nullif(DBT_INTERNAL_DEST.{{ key }}, '')
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                nullif(DBT_INTERNAL_SOURCE.{{ unique_key }}, '') = nullif(DBT_INTERNAL_DEST.{{ unique_key }}, '')
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}
 
    {{ sql_header if sql_header is not none }}
 
    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}
        {{ fl_utils.add_merge_update_condition() }}
    {% if unique_key %}
    when matched {{ fl_utils.add_merge_update_condition() }} then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}
 
    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})
 
{% endmacro %}


{% macro add_merge_update_condition() %} 
	{% set col = config.get('update_condition', none) %} 
	{% if col %} and DBT_INTERNAL_DEST.{{col}} = 'Y'
	{% endif %} 
{% endmacro %}

