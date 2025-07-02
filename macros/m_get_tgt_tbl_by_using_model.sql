{% macro m_get_tgt_tbl_by_using_model(model_grp, model_id) %}
  {# Fetch Full length target table name using the model id from source yaml #}
  {% if execute %}
    {% set tgt_tbl = None %}
    {% for source in graph.sources.values() %}
      {% if source.source_name == model_grp %}
        {% if source.name == model_id %}
          {% set tgt_tbl = source.meta.tgt_tbl %}
          {{ return(tgt_tbl) }}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
