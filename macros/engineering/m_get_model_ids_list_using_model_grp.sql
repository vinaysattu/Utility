{% macro m_get_model_ids_list_using_model_grp(model_group) %}
    {# Fetch list of Model IDs from the source.yml using model_group with macro (m_get_model_ids_list_using_model_grp) #}
    {%- if execute -%}
        {% set tables = [] %}
        {% for source in graph.sources.values() %}
            {%- if source.source_name == model_group -%}
                {% set tbl_item = source.name %}
                {% do tables.append(tbl_item) %}
            {%- endif -%}
        {% endfor %}
        
        {% if tables %}
            {{ return(tables) }}
        {% else %}
            {{ return([]) }}
        {% endif %}
    {%- endif -%}
{% endmacro %}