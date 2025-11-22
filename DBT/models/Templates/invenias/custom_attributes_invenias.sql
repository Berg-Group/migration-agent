{{ config(
    materialized='table',
    alias='custom_attributes_invenias'
) }}

SELECT
    {{ atlas_uuid('c.itemid') }} AS atlas_id,
	c.fileas AS name,
    '2025-06-09T00:00:00' AS created_at,
    '2025-06-09T00:00:00' AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    'person' AS of,
    'options' AS type,
    FALSE AS ai
FROM 
    {{ var('source_database') }}."categorylists" c