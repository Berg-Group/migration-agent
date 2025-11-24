{{ config(materialized='table', alias='experiences_concat_rcrm') }}

{% set db = var('source_database') %}

WITH 

{% set work_history_tables = [] %}
{% for i in range(1, 51) %}
    {% set table_name = 'work_history_' ~ i ~ '_data' %}
    {% if execute %}
        {% set check_query %}
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = '{{ db }}' 
            AND table_name = '{{ table_name }}'
        {% endset %}
        {% set results = run_query(check_query) %}
        {% if results and results[0][0] > 0 %}
            {% do work_history_tables.append(table_name) %}
        {% endif %}
    {% endif %}
{% endfor %}

{% if work_history_tables|length == 0 and execute %}
    {% set check_query %}
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{{ db }}' 
        AND table_name = 'work_history_data'
    {% endset %}
    {% set results = run_query(check_query) %}
    {% if results and results[0][0] > 0 %}
        {% do work_history_tables.append('work_history_data') %}
    {% endif %}
{% endif %}

{% for table_name in work_history_tables %}
    {{ "," if not loop.first }}
    {{ table_name | replace('-', '_') }}_source AS (
        SELECT 
            candidate_slug,
            work_start_date,
            work_end_date,
            title,
            description,
            work_company_name,
            '{{ table_name }}' AS source_table
        FROM 
            {{ db }}.{{ table_name }}
    )
{% endfor %}

{% if work_history_tables|length == 0 %}
-- No work history tables found
empty_result AS (
    SELECT 
        NULL::VARCHAR AS candidate_slug,
        NULL::VARCHAR AS work_start_date,
        NULL::VARCHAR AS work_end_date,
        NULL::VARCHAR AS title,
        NULL::VARCHAR AS description,
        NULL::VARCHAR AS work_company_name,
        'no_tables_found' AS source_table
    WHERE 1=0
)
{% endif %}

-- Select all data
{% if work_history_tables|length > 0 %}
    {% for table_name in work_history_tables %}
        {{ "UNION ALL" if not loop.first }}
        SELECT * FROM {{ table_name | replace('-', '_') }}_source
    {% endfor %}
{% else %}
    SELECT * FROM empty_result
{% endif %}
