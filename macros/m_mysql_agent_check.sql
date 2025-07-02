{% macro m_mysql_agent_check(p_model_grp, p_model_id) %}
{%- set v_model_grp = p_model_grp %}
{%- set v_model_id = p_model_id %}
{%- set retries = 5 %}
{%- set sleep_seconds = 60 %}

{%- set v_meta_dict = fl_utils.m_get_meta_details_using_model_grp( v_model_grp ) %}
{%- set v_mysql_connector_db = v_meta_dict.mysql_connector_db %}

{%- for attempt in range(retries) %}
    {%- set replication_status_query %}
        select
            status
        from
            {{ v_mysql_connector_db }}
        where
            trim(lower(resource_name)) = trim(lower('{{v_model_id}}'))
            {# once replication is more active for orders, we can un comment these, commenting out for testing#}
            {# and started_at >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
            and started_at < DATE_TRUNC('hour', DATEDIFF(hour, 1, CURRENT_TIMESTAMP)) #}
        ORDER BY
            started_at DESC
        LIMIT 1
    {%- endset %}

    {%- set results = run_query(replication_status_query) %}
    {%- set status = results.columns[0].values()[0] if results.columns[0].values() else 'not found' %}

    {%- if status == 'running' %}
        {% do log("Replication is currently running. Attempt " ~ (attempt + 1) ~ " of " ~ retries, info=True) %}
        {%- if attempt < retries - 1 %}
            {% do log("Sleeping for " ~ sleep_seconds ~ " seconds before retrying...", info=True) %}
            {%- do run_query("CALL SYSTEM$WAIT(" ~ sleep_seconds ~ ")") %}
        {%- else %}
            {% do log("Max retries reached. Exiting...", info=True) %}
            {%- do return(None) %}
        {%- endif %}
    {%- else %}
        {% do log("Replication Status: " ~ status, info=True) %}
        {%- do return(status) %}
    {%- endif %}
{%- endfor %}
{% endmacro %}
