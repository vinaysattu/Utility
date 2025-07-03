{% macro m_get_meta_details_using_model_grp(p_model_grp) %}
    {# Extract the entire meta details for the specified model_grp from the source yaml file #}
    {% if execute %}
        {% for source in graph.sources.values() %}
            {% if source.source_name == p_model_grp %}
                {% if source.meta is not none %}
                    {{ return(source.meta) }}
                {% else %}
                    {{ return({}) }} {# Return an empty dictionary if meta is not defined #}
                {% endif %}
            {% endif %}
        {% endfor %}
        {{ return({}) }} {# Return an empty dictionary if no matching model_grp is found #}
    {% endif %}
{% endmacro %}