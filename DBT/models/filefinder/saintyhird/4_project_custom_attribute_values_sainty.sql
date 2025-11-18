{{ config(
    materialized='table',
    alias='project_custom_attribute_values_sainty',
    tags=["saintyhird"]
) }}

WITH internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id
    FROM {{ ref('12_projects_ff') }}
),
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        ca.alias AS attribute_alias,
        cao.atlas_id AS option_id,
        cao.value AS option_value
    FROM {{ ref('2_custom_attribute_options_sainty') }} cao
    INNER JOIN {{ ref('1_custom_attributes_sainty') }} ca 
        ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'project'
),
project_industry_values AS (
    SELECT DISTINCT a.idassignment AS project_id, i.value AS value, 'project_industry' AS attribute_alias
    FROM {{ var('source_database') }}."assignment" a
    INNER JOIN {{ var('source_database') }}.assignmentcode ac ON ac.idassignment = a.idassignment
    INNER JOIN {{ var('source_database') }}.industry i ON i.idindustry = ac.codeid
    WHERE i.isactive = 1 AND i.value IS NOT NULL AND TRIM(i.value) != ''
),
project_function_values AS (
    SELECT DISTINCT a.idassignment AS project_id, j.value AS value, 'project_function' AS attribute_alias
    FROM {{ var('source_database') }}."assignment" a
    INNER JOIN {{ var('source_database') }}.assignmentcode ac ON ac.idassignment = a.idassignment
    INNER JOIN {{ var('source_database') }}.jobfunction j ON j.idjobfunction = ac.codeid
    WHERE j.isactive = 1 AND j.value IS NOT NULL AND TRIM(j.value) != ''
),
project_international_values AS (
    SELECT DISTINCT a.idassignment AS project_id, i.value AS value, 'project_international' AS attribute_alias
    FROM {{ var('source_database') }}."assignment" a
    INNER JOIN {{ var('source_database') }}.assignmentcode ac ON ac.idassignment = a.idassignment
    INNER JOIN {{ var('source_database') }}.international i ON i.idinternational = ac.codeid
    WHERE i.isactive = 1 AND i.value IS NOT NULL AND TRIM(i.value) != ''
),
combined_project_values AS (
    SELECT * FROM project_industry_values
    UNION ALL SELECT * FROM project_function_values
    UNION ALL SELECT * FROM project_international_values
)
SELECT DISTINCT
    {{ atlas_uuid('ip.project_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ip.project_id,
    ip.atlas_project_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM combined_project_values cpv
INNER JOIN internal_projects ip ON ip.project_id = cpv.project_id
INNER JOIN internal_options io 
    ON io.attribute_alias = cpv.attribute_alias 
    AND io.option_value = cpv.value
ORDER BY ip.project_id, io.atlas_attribute_id


