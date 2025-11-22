{{ config(
    materialized='table',
    alias='custom_attributes_inte',
    tags = ["bullhorn"]
) }}

WITH custom_attributes AS (
    SELECT 'Pronouns' AS entity_name
    UNION ALL
    SELECT 'Primary Skill' AS entity_name
    UNION ALL
    SELECT 'Education Level' AS entity_name
    UNION ALL
    SELECT 'Current Location' AS entity_name
    UNION ALL
    SELECT 'Work Authority' AS entity_name
    UNION ALL
    SELECT 'Standard Job Title' AS entity_name
    UNION ALL
    SELECT 'Visa Sponsor/ Transfer?' AS entity_name
    UNION ALL
    SELECT 'Account Manager' AS entity_name
)
SELECT
    {{ atlas_uuid("'custom' || entity_name || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Pronouns' THEN 'pronouns'
        WHEN entity_name = 'Primary Skill' THEN 'primary_skill'
        WHEN entity_name = 'Education Level' THEN 'education_level'
        WHEN entity_name = 'Current Location' THEN 'current_location'
        WHEN entity_name = 'Work Authority' THEN 'work_authority'
        WHEN entity_name = 'Standard Job Title' THEN 'standard_job_title'
        WHEN entity_name = 'Visa Sponsor/ Transfer?' THEN 'visa_sponsor_transfer'
        WHEN entity_name = 'Account Manager' THEN 'account_manager'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    CASE 
        WHEN entity_name In ('Visa Sponsor/ Transfer?', 'Account Manager') THEN FALSE 
        ELSE TRUE 
    END AS multiple_values,
    FALSE AS ai,
    'options' AS type,
    CASE 
        WHEN entity_name IN ('Pronouns', 'Primary Skill', 'Education Level', 'Current Location', 'Work Authority', 'Standard Job Title') THEN 'person'
        WHEN entity_name IN ('Visa Sponsor/ Transfer?') THEN 'project'
        WHEN entity_name IN ('Account Manager') THEN 'company'
        ELSE 'person'
    END AS of
FROM custom_attributes