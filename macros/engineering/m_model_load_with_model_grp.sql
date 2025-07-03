{% macro m_model_load_with_model_grp(p_model_grp, p_model_name='ALL', p_is_backfill='N') %}

	{% set v_model_grp = p_model_grp %}
	{% set v_model_name = p_model_name %}
	{% set v_is_backfill = p_is_backfill %}
	
    {# Extract attribute values of model_grp (source). from source yaml file #}
    {% set v_meta_dict = fl_utils.m_get_meta_details_using_model_grp( v_model_grp ) %}
    {% set v_pipeline_name = v_meta_dict.pipeline_name %}
    {% set v_tgt_layer = v_meta_dict.tgt_layer %}
    {% do log("Pipeline Name: " ~ v_pipeline_name, info=True) %}
    {% do log("Load Layer: " ~ v_tgt_layer, info=True) %}
    {% do log("Model Group: " ~ v_model_grp, info=True) %}
    
	{# Fetch Batch Id using the pipeline name from dbt_monitor.dbt_model_batch_t #}
    {% set v_batch_id = fl_utils.m_get_batch_id( v_pipeline_name ) %}
    
	{# Fetch the batch status using the pipeline name from dbt_monitor.dbt_model_batch_t #}
    {% set v_batch_status = fl_utils.m_get_batch_status( v_pipeline_name, v_batch_id ) %}
    
	{% do log("Load Batch Id: " ~ v_batch_id ~ ", Batch Status: " ~ v_batch_status, info=True) %}
	
	{# Fetch list of Model IDs from the source.yml using model_group #}
    {% set v_model_id_list = fl_utils.m_get_model_ids_list_using_model_grp( v_model_grp ) %}

    {# Filter model_id_list if model_name is not ALL #}
    {% if v_model_name != 'ALL' %}
        {% set v_model_id_list = v_model_id_list | select( "equalto", v_model_name ) %}
    {% endif %}
    {% do log("model_list: " ~ v_model_id_list, info=True) %}

    {% if v_batch_status == 'running' %}
 
        {# Initiate all the load attributes inside the table dbt_monitor.dbt_model_audit_t #}
        {{ fl_utils.m_init_load_attrib_vals_for_all_models_in_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_list, v_batch_id ) }}
        commit;
        {% do log("All the Model meta data is Initated, Proceeding with the model load process..", info=True) %}

        {% for v_model_id in v_model_id_list %}

            {% do log("---------------------------------------------------------", info=True) %}

            {# Fetch Full length target table name using the model id from yaml #}
            {% set v_model_id_full = fl_utils.m_get_tgt_tbl_by_using_model( v_model_grp, v_model_id ) %}   

            {# Fetch current batch_id from the (dbt_monitor.dbt_model_audit_t) Table #}
            {% set t_batch_id = fl_utils.m_get_batch_id_for_model_record_from_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) %}

            {# Fetch Model load status from the (dbt_monitor.dbt_model_audit_t) Table #}
            {% set t_model_load_status = fl_utils.m_get_load_stat_for_model_record_from_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) %}
            
            {% if v_batch_id == t_batch_id and t_model_load_status  == 'completed' %} 
                
				{% do log("The Model " ~ v_model_id ~ ", data is already loaded, Skipping the model..", info=True) %}
            
			{% else %}

                {# Fetch inc_load_ts from the (dbt_monitor.dbt_model_audit_t) Table #}
                {% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) %}

                {# Pre-set (Update) the model load attributes for this model #}
                {{ fl_utils.m_upd_pre_load_attrib_vals_for_model_record_in_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) }}

                {% if v_pipeline_name == 'mfcs' %}   
                    
					{% if v_tgt_layer == 'stage' %}
 
                        {# Load from External Stage to stage #}
                        {{ fl_utils.m_dynamic_load_extstage_to_stage_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full ) }}
 
                    {% elif v_tgt_layer == 'bronze' %}
 
                        {# Load from Stage to Bronze for each model sequentially with the incremental strategy specific to the model.#}
                        {{ fl_utils.m_dynamic_incload_stage_to_bronze_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id ) }}

                    {% elif v_tgt_layer == 'silver' %}

                        {# Load from Bronze to Silver for each model sequentially with the incremental strategy specific to the model. #}
                        {{ fl_utils.m_dynamic_incload_bronze_to_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id ) }}

                        {# Deletion of Silver Table records (Hard Delete), The below macro compares and fetches all the deleted records by doing target-source,if the records are deleted in stage by OGG, these delete records are then moved to tbl_del_t tables in the same silver layer, and then deleted fromt the target table. #}
                        {{ fl_utils.m_process_deletions_from_silver_using_del_flg( v_pipeline_name, v_model_grp, v_model_id ) }}   

                    {% endif %}

				{% elif v_pipeline_name == 'wfm' %}

					{% if v_tgt_layer == 'bronze' %}          

						{# Load from External Stage to Bronze for each model sequentially, considering the specific atttributes for a model from yaml.#}
						{{ fl_utils.m_dynamic_load_extstage_to_bronze_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) }}

					{% elif v_tgt_layer == 'silver' %}

						{# Load from Bronze to Silver (Delete and then Insert) for each model sequentially, considering the specific atttributes for a model from yaml.#}
						{{ fl_utils.m_dynamic_delinsload_bronze_to_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) }}

                    {% endif %}

				{% elif v_pipeline_name == 'pim' or v_pipeline_name == 'pim_v9' %}

					{% if v_tgt_layer == 'silver' %}          

                        {# Load which is pre-validated(Divert bad records to dbt_monitor.dbt_model_pre_validation_check_t table for the checks in place) from Bronze to Silver for each model sequentially with the incremental strategy specific to the model. #}
                        {{ fl_utils.m_dynamic_prevalidated_incload_bronze_to_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id ) }}  
                        
                    {% endif %}

                {% elif v_pipeline_name == 'dom' or v_pipeline_name == 'wm' %}

                    {% if v_tgt_layer == 'bronze' %} 

                        {{ fl_utils.m_dynamic_incload_mysql_journal_to_bronze_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id) }}

                    {% elif v_tgt_layer == 'silver' %}

                        {% if v_is_backfill == 'Y' %} 

                            {{ fl_utils.m_dynamic_backfill_mysql_bronze_to_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id) }}

                        {% elif v_is_backfill == 'N' %}

                            {{ fl_utils.m_dynamic_incload_mysql_stage_to_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id) }}

                        {% endif %}

                        {{ fl_utils.m_mysql_process_deletions_from_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id) }}

                    {% endif %}

                {% elif v_pipeline_name == 'store_health' %}   
                    
					{% if v_tgt_layer == 'silver' %}

                        {# Load from Bronze to Silver for each model sequentially with the incremental strategy specific to the model. #}
                        {{ fl_utils.m_dynamic_incload_bronze_to_silver_using_model_id( v_pipeline_name, v_model_grp, v_model_id_full, v_inc_load_ts, v_batch_id ) }}

                    {% endif %}

				{% else %}
				
					{% do log("Model group did not match any known pattern.", info=True) %}
				
				{% endif %}

                {# Post the data load, post-set (Update) the model load attributes for this model #}
                {{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) }}
                {# Fetch rows_inserted from the (dbt_monitor.dbt_model_audit_t) Table #}
                {% set rows_inserted = fl_utils.m_get_rows_inserted_for_model_record_from_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) %}
                {# Fetch rows_updated from the (dbt_monitor.dbt_model_audit_t) Table #}
                {% set rows_updated = fl_utils.m_get_rows_updated_for_model_record_from_dbt_model_audit( v_pipeline_name, v_model_grp, v_model_id_full, v_batch_id ) %}

                {% do log("Current Batch Records Inserted: " ~  rows_inserted, info=True) %}
                {% do log("Current Batch Records  Updated: " ~ rows_updated, info=True) %}                 
			{% endif %}
            
            {% do log("---------------------------------------------------------", info=True) %}	

        {% endfor %}  

    {% else %}

        {% do log("Batch Status is not running, skipping model processing.", info=True) %}

    {% endif %}

{% endmacro %}