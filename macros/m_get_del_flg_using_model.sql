{% macro m_get_del_flg_using_model(model_grp, model_id) %}
  {# Fetch the Delete flag from the source yaml file, where it is Y or N using the model #}
  {% if execute -%}
    {% set stg_tbl = 'n' %}
    {% for source in graph.sources.values() %}
      {% if source.source_name == model_grp -%}
        {% if source.name == model_id -%}
          {% set del_flg = source.meta.del_flg %}
          {{ return(del_flg) }}
        {% endif -%}
      {% endif -%}
    {% endfor %}
  {% endif -%}
{% endmacro %}
