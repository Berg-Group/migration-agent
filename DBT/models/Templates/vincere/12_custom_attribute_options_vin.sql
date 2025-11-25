{{ config(
    materialized='table',
    alias='custom_attribute_options_vincere'
) }}

WITH custom_attributes AS (
    SELECT
        atlas_id AS atlas_attribute_id,
        name AS attribute_name
    FROM {{ref('11_custom_attributes_vin')}} 
)

    SELECT
        cg.id AS id,
        cg.name AS value,
        ROW_NUMBER() OVER (ORDER BY cg.name) AS position,
        ca.atlas_attribute_id,
        '{{ var('agency_id') }}' AS agency_id,
        '{{ var("date") }}T00:00:00' AS created_at,
        '{{ var("date") }}T00:00:00' AS updated_at,
        {{ atlas_uuid("'atlas_attribute_id' || '{{ var(\"agency_id\") }}' || id::text") }} AS atlas_id,
        TRUE AS multiple_values,
        'person' AS of
    FROM
        {{ var('source_database') }}."candidate_group" cg
    CROSS JOIN 
        custom_attributes ca
