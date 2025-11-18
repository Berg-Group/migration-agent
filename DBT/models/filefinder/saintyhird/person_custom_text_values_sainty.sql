{{ config(
    materialized='table',
    alias='person_custom_text_values_sainty',
    tags=["saintyhird"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM {{ ref('2_people_ff') }}
),
internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM {{ ref('custom_attributes_text_sainty') }}
    WHERE of = 'person'
),
known_as_values AS (
    SELECT DISTINCT p.idperson AS person_id, p.knownas AS value, ca.alias AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN internal_attributes ca ON ca.alias = 'known_as'
    WHERE p.knownas IS NOT NULL AND TRIM(p.knownas) != ''
),
maiden_name_values AS (
    SELECT DISTINCT p.idperson AS person_id, p.maidenname AS value, ca.alias AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN internal_attributes ca ON ca.alias = 'maiden_name'
    WHERE p.maidenname IS NOT NULL AND TRIM(p.maidenname) != ''
),
date_of_birth_values AS (
    SELECT DISTINCT p.idperson AS person_id, TO_CHAR(p.dateofbirth, 'YYYY-MM-DD') AS value, ca.alias AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN internal_attributes ca ON ca.alias = 'date_of_birth'
    WHERE p.dateofbirth IS NOT NULL
),
combined_person_values AS (
    SELECT * FROM known_as_values
    UNION ALL SELECT * FROM maiden_name_values
    UNION ALL SELECT * FROM date_of_birth_values
)
SELECT DISTINCT
    {{ atlas_uuid('ip.person_id::text || cpv.value::text || ia.atlas_id::text') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    ia.atlas_id AS atlas_custom_attribute_id,
    cpv.value AS value,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM combined_person_values cpv
INNER JOIN internal_persons ip ON ip.person_id = cpv.person_id
INNER JOIN internal_attributes ia ON ia.alias = cpv.attribute_alias
ORDER BY ip.person_id, ia.atlas_id


