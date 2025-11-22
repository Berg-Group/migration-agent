{{ config(
    materialized='table',
    alias='custom_attributes_rcrm',
    tags = ["recruit_crm"]
) }}

WITH hotlist_entity AS (
    SELECT DISTINCT entity_name
    FROM 
        {{ var('source_database') }}.hotlist_data
)

SELECT
    {{ atlas_uuid("'hotlist' || entity_name || '" ~ var('agency_id') ~ "'") }} AS atlas_id,
    CASE 
        WHEN entity_name = 'candidates' THEN 'Candidate Hotlist'
        WHEN entity_name = 'contacts' THEN 'Contact Hotlist'
        WHEN entity_name = 'companies' THEN 'Company Hotlist'
        ELSE 'hotlist'
    END AS name,
    entity_name AS alias,
    '2025-06-03T00:00:00' AS created_at,
    '2025-06-03T00:00:00' AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    CASE 
        WHEN entity_name IN ('candidates', 'contacts') THEN 'person'
        WHEN entity_name = 'companies' THEN 'company'
        ELSE 'person'
    END AS of
FROM hotlist_entity
