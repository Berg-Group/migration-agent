{{ config(
    materialized='table',
    alias='people_rect',
    tags=['recruitly']
) }}

WITH candidates AS (
    SELECT
        c.candidate_id AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.candidate_id::text") }} AS atlas_id,
        NULLIF(TRIM(c.first_name), '') AS first_name,
        NULLIF(TRIM(c.surname), '') AS last_name,
        TRIM(c.internal_overview) AS overview,
        NULL AS location_street_address,
        NULLIF(TRIM(c.city), '') AS location_locality,
        NULLIF(TRIM(c.region), '') AS location_region,
        NULLIF(TRIM(c.country), '') AS location_country,
        NULLIF(TRIM(c.postcode), '') AS location_postal_code,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at,
        u.id AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        u.id AS updated_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.candidates c
    LEFT JOIN {{ ref('1_users_rect') }} u ON u.id = c.owner_id
),
contacts AS (
    SELECT
        c.contact_id AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.contact_id::text") }} AS atlas_id,
        NULLIF(TRIM(c.first_name), '') AS first_name,
        NULLIF(TRIM(c.surname), '') AS last_name,
        TRIM(c.description) AS overview,
        NULLIF(TRIM(c.address_line), '') AS location_street_address,
        NULLIF(TRIM(c.city), '') AS location_locality,
        NULLIF(TRIM(c.region), '') AS location_region,
        NULLIF(TRIM(c.country), '') AS location_country,
        NULLIF(TRIM(c.postcode), '') AS location_postal_code,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at,
        u.id AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        u.id AS updated_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.contacts c
    LEFT JOIN {{ ref('1_users_rect') }} u ON u.id = c.owner_id
)
SELECT * FROM candidates
UNION ALL
SELECT * FROM contacts