{{ config(
    materialized='table',
    alias='project_custom_attribute_values_neuco',
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
        {{ ref('2_custom_attribute_options_neuco') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_neuco') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'project'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
service_type_mappings AS (
    SELECT DISTINCT
        j.JobPostingID AS project_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(j.customtext3, ';', ','), ',,', ','), ',', numbers.n)) AS service_type_value
    FROM {{ var('source_database') }}.bh_jobopportunity j
    CROSS JOIN numbers
    WHERE j.customtext3 IS NOT NULL
        AND TRIM(j.customtext3) != ''
        AND SPLIT_PART(REPLACE(REPLACE(j.customtext3, ';', ','), ',,', ','), ',', numbers.n) != ''
),
confidential_role_mappings AS (
    SELECT DISTINCT
        j.JobPostingID AS project_id,
        j.customtext5 AS confidential_role_value
    FROM {{ var('source_database') }}.bh_jobopportunity j
    WHERE j.customtext5 IS NOT NULL AND j.customtext5 != ''
),
role_type_mappings AS (
    SELECT DISTINCT
        j.JobPostingID AS project_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.newvalue, ';', ','), ',,', ','), ',', numbers.n)) AS role_type_value
    FROM {{ var('source_database') }}.bh_jobopportunity j
    INNER JOIN {{ var('source_database') }}.bh_jobpostingedithistory h ON h.jobpostingid = j.jobpostingid
    INNER JOIN {{ var('source_database') }}.bh_jobpostingedithistoryfieldchange c ON c.jobpostingedithistoryid = h.jobpostingedithistoryid
    CROSS JOIN numbers
    WHERE c.display = 'Role Type' 
        AND c.newvalue IS NOT NULL 
        AND c.newvalue != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.newvalue, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_mappings AS (
    SELECT 
        project_id,
        'service_type'::text AS attribute_type,
        service_type_value AS value
    FROM service_type_mappings
    UNION ALL
    SELECT 
        project_id,
        'confidential_role'::text AS attribute_type,
        confidential_role_value AS value
    FROM confidential_role_mappings
    UNION ALL
    SELECT 
        project_id,
        'project_role_type'::text AS attribute_type,
        role_type_value AS value
    FROM role_type_mappings
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