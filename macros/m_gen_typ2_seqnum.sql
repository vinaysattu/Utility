{% macro m_gen_typ2_seqnum(model_id, seqno_col) %}

    -- SQL expression to generate Type-2 Sequence number key or a version key
	nvl(
		(select max(tgt.{{ seqno_col }}) 
		from {{ model_id }} tgt 
		where tgt.hash_sk=src.hash_sk 
			and tgt.hash_seq_no=src.hash_seq_no 
			and tgt.active_flg = 'Y')
        ,row_number() over (order by null) +  
		(select coalesce(max(t.{{ seqno_col }}), 0) as max_row 
		from {{ model_id }} t 
		where t.hash_sk=src.hash_sk)
	)::number(38,0)

{% endmacro %}