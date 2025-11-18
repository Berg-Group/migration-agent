{{ config(
    materialized='table',
    alias='custom_attributes_clarity',
    tags = ["recruit_crm"]
) }}

WITH custom_attributes AS (
    SELECT 'Specialization' AS entity_name
    UNION ALL
    SELECT 'Sub Industry' AS entity_name
)

SELECT
    {{ atlas_uuid("'company_custom' || entity_name || '" ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    entity_name AS alias,
    '2025-06-03T00:00:00' AS created_at,
    '2025-06-03T00:00:00' AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    FALSE AS ai,
    'company' AS of
FROM custom_attributes