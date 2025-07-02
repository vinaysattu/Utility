{% macro m_get_stg_tbl_by_using_model(model_grp, model_id) %}
  {# Fetch Full length stage table name using the model id from source yaml #}
  {% if execute %}
    {% set stg_tbl = None %}
    {% for source in graph.sources.values() %}
      {% if source.source_name == model_grp %}
        {% if source.name == model_id %}
          {% set stg_tbl = source.meta.stg_tbl %}
          {{ return(stg_tbl) }}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
