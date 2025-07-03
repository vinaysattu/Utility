{% macro m_apply_typ1_deletion(src_tbl, tgt_tbl, src_to_tgt_join_string ) %}

	{% set fully_qualified_name_str = src_tbl | string %}
	{% set parts = fully_qualified_name_str.split('.') %}
	{% set src_db = parts[0] %}
	{% set src_schema = parts[1] %}
	{% set src_tbl_short = parts[2] %}

	{# Prepares deletion table name, using the Target table, which is of same structure of target #}
	{% set table_base = src_tbl_short[:src_tbl_short|length - 2] %}
	{% set del_table_name = table_base ~ '_del_t' %}

	{# Step 2: Hard delete these records from the silver table #}
	{% set delete_sql -%}    
		begin;
		delete from {{ tgt_tbl }} t
		where
			exists (
				select 'x'
				from {{ src_db }}.{{ src_schema }}.{{ del_table_name }} s
				where ({{ src_to_tgt_join_string }})
			);
		commit;
	{%- endset %}
	{% do run_query(delete_sql) %}  

{% endmacro %}