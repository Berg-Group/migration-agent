{{ config(materialized='table', alias='educations_concat_rcrm') }}

{% set db = var('source_database') %}

WITH 

{% set education_history_tables = [] %}
{% for i in range(1, 5) %}
    {% set table_name = 'education_history_' ~ i ~ '_data' %}
    {% if execute %}
        {% set check_query %}
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = '{{ db }}' 
            AND table_name = '{{ table_name }}'
        {% endset %}
        {% set results = run_query(check_query) %}
        {% if results and results[0][0] > 0 %}
            {% do education_history_tables.append(table_name) %}
        {% endif %}
    {% endif %}
{% endfor %}

{% if education_history_tables|length == 0 and execute %}
    {% set check_query %}
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{{ db }}' 
        AND table_name = 'education_history_data'
    {% endset %}
    {% set results = run_query(check_query) %}
    {% if results and results[0][0] > 0 %}
        {% do education_history_tables.append('education_history_data') %}
    {% endif %}
{% endif %}

{% for table_name in education_history_tables %}
    {{ "," if not loop.first }}
    {{ table_name | replace('-', '_') }}_source AS (
        SELECT 
            candidate_slug,
            institute_name,
            educational_qualification,
            educational_specialization,
            education_description,
            education_start_date,
            education_end_date,
            '{{ table_name }}' AS source_table
        FROM 
            {{ db }}.{{ table_name }}
    )
{% endfor %}

{% if education_history_tables|length == 0 %}
-- No education history tables found
empty_result AS (
    SELECT 
        NULL::VARCHAR AS candidate_slug,
        NULL::VARCHAR AS institute_name,
        NULL::VARCHAR AS educational_qualification,
        NULL::VARCHAR AS educational_specialization,
        NULL::VARCHAR AS education_description,
        NULL::VARCHAR AS education_start_date,
        NULL::VARCHAR AS education_end_date,
        'no_tables_found' AS source_table
    WHERE 1=0
)
{% endif %}

-- Select all data
{% if education_history_tables|length > 0 %}
    {% for table_name in education_history_tables %}
        {{ "UNION ALL" if not loop.first }}
        SELECT * FROM {{ table_name | replace('-', '_') }}_source
    {% endfor %}
{% else %}
    SELECT * FROM empty_result
{% endif %}
