{{ config(
    materialized='table',
    alias='project_custom_attribute_values_inte',
    tags = ["bullhorn"]
) }}

WITH internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id
    FROM 
        {{ ref('10_projects_bh') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_inte') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_inte') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'project'
),
visa_sponsor_transfer_values AS (
    SELECT DISTINCT
        j.JobPostingID AS project_id,
        CASE 
            WHEN j.willsponsor = 0 THEN 'Yes'
            WHEN j.willsponsor = 1 THEN 'No'
        END AS value
    FROM {{ var('source_database') }}.bh_jobopportunity j
    WHERE j.willsponsor IN (0, 1)
),
combined_mappings AS (
    SELECT 
        project_id,
        'visa_sponsor_transfer'::text AS attribute_type,
        value
    FROM visa_sponsor_transfer_values
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
FROM 
    combined_mappings cm
INNER JOIN 
    internal_projects ip ON ip.project_id = cm.project_id
INNER JOIN 
    internal_options io ON io.attribute_type = cm.attribute_type AND io.option_value = cm.value
ORDER BY
    project_id,
    atlas_custom_attribute_id 