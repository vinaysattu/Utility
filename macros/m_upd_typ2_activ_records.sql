{% macro m_upd_typ2_activ_records(tgt_tbl , sk, seq_num) %}

    {% set update_sql1 -%}
        begin;
		update {{ tgt_tbl }} t 
		set t.end_ts = v.end_ts,
			t.active_flg = case when v.end_ts is null then 'Y' else 'N' end,
            t.reporting_flg = case when v.end_ts is null then 'Y' else 'N' end
		from  
			(select {{ sk }},{{ seq_num }},start_ts, 
				coalesce(lead(start_ts) over (partition by {{ sk }} order by start_ts),null) as end_ts
			from {{ tgt_tbl }}              
			where active_flg = 'Y') as v
		where t.{{ sk }} = v.{{ sk }}
			and t.{{ seq_num }} = v.{{ seq_num }}
			and t.start_ts = v.start_ts
			and t.active_flg = 'Y';
        commit;
    {%- endset %}
    {% do run_query(update_sql1) %}  

    {% set update_sql2 -%}
        begin;
        update {{ tgt_tbl }} t
        set t.reporting_flg = 'N'
        from (
            select {{ sk }},{{ seq_num }}, max(start_ts) as start_ts
            from {{ tgt_tbl }}
            where reporting_flg = 'Y'
            group by all
        ) as v
        where t.{{ sk }} = v.{{ sk }}
            and t.{{ seq_num }} = v.{{ seq_num }}
            and t.start_ts < v.start_ts
            and t.reporting_flg = 'Y';
       commit;
    {%- endset %}
    {% do run_query(update_sql2) %}  

{% endmacro %}