{{ config(
    materialized='table',
    alias='project_custom_attribute_values_invecta',
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
        {{ ref('2_custom_attribute_options_invecta') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_invecta') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'project'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
project_job_title_values AS (
    SELECT DISTINCT
        j.JobPostingID AS project_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(j.customText1, ';', ','), ',,', ','), ',', numbers.n)) AS project_job_title_value
    FROM {{ var('source_database') }}.bh_jobopportunity j
    CROSS JOIN numbers
    WHERE LENGTH(TRIM(j.customText1)) > 1
        AND SPLIT_PART(REPLACE(REPLACE(j.customText1, ';', ','), ',,', ','), ',', numbers.n) != ''
),
project_specialism_values AS (
    SELECT DISTINCT 
        c.JobPostingID AS project_id,
        sl."name" AS project_specialism_value
    FROM {{ var('source_database') }}.bh_jobopportunity c
    INNER JOIN {{ var('source_database') }}.bh_jobskill s ON s.jobpostingid = c.jobpostingid  
    INNER JOIN {{ var('source_database') }}.bh_skilllist sl ON sl.skillid = s.skillid 
    WHERE sl."name" IS NOT NULL AND sl."name" != ''
),
project_skillset_values AS (
    SELECT DISTINCT
        j.JobPostingID AS project_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(j.customText2, ';', ','), ',,', ','), ',', numbers.n)) AS project_skillset_value
    FROM {{ var('source_database') }}.bh_jobopportunity j
    CROSS JOIN numbers
    WHERE j.customText2 IS NOT NULL AND TRIM(j.customText2) != ''
        AND SPLIT_PART(REPLACE(REPLACE(j.customText2, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_mappings AS (
    SELECT 
        project_id,
        'project_job_title'::text AS attribute_type,
        project_job_title_value AS value
    FROM project_job_title_values
    UNION ALL
    SELECT 
        project_id,
        'project_specialism'::text AS attribute_type,
        project_specialism_value AS value
    FROM project_specialism_values
    UNION ALL
    SELECT 
        project_id,
        'project_skillset'::text AS attribute_type,
        project_skillset_value AS value
    FROM project_skillset_values
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