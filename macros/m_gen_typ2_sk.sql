{% macro m_gen_typ2_sk(model_id, sk_col, key_columns) %}

    -- Convert the key columns list into a comma-separated string
    {%- set key_columns_str = key_columns | join(', ') -%}

    -- SQL expression to generate Type-2 surrogate key
    nvl(
        (select max(tgt.{{ sk_col }}) 
         from {{ model_id }} tgt 
         where tgt.hash_sk = src.hash_sk),
        dense_rank() over (order by {{ key_columns_str }}) + 
        (select coalesce(max(t.{{ sk_col }}), 0) as max_row 
         from {{ model_id }} t)
    )::number(38,0)

{% endmacro %}