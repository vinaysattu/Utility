{% macro m_prep_hash_key_from_column_list(target_key_column_list) %}

    {% for i in target_key_column_list %}

        {% if loop.first %}  
            sha2_hex(nullif(concat( ifnull(nullif(upper(trim(src.{{ i }}::varchar)), ''), '^^') 
        {% else %}
             , '||', ifnull(nullif(upper(trim(src.{{ i }}::varchar)), ''), '^^') 
        {% endif %}    

    {% endfor %}
    ), '^^||^^'))::varchar(100)

{% endmacro %}