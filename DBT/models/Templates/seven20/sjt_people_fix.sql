{{ config(
    materialized='table',
    alias='sjt_people_fix',
    tags=["seven20"]
) }}

WITH duplicated_values AS (
    SELECT value
    FROM {{ this.schema }}.person_identities
    WHERE type = 'phone'
    GROUP BY value
    HAVING COUNT(*) > 1
),

duplicated_identities AS (
    SELECT pi.person_id, 
    pi.value, 
    p.first_name, 
    p.last_name, 
    p.created_by,
    p.created_by_atlas_id,
    p.location_locality,
    p.created_at,
    p.updated_at
    FROM {{ this.schema }}.person_identities pi
    INNER JOIN {{ this.schema }}.people p ON p.id = pi.person_id
    INNER JOIN duplicated_values dv ON pi.value = dv.value
    WHERE pi.type = 'phone'
),

generated_people AS (
    SELECT 
        person_id,
        LOWER(
            SUBSTRING(MD5(p.person_id::text || current_date::text), 1, 8) || '-' ||
            SUBSTRING(MD5(p.person_id::text || current_date::text), 9, 4) || '-' ||
            SUBSTRING(MD5(p.person_id::text || current_date::text), 13, 4) || '-' ||
            SUBSTRING(MD5(p.person_id::text || current_date::text), 17, 4) || '-' ||
            SUBSTRING(MD5(p.person_id::text || current_date::text), 21, 12)
        ) AS atlas_id, 
        p.first_name,
        p.last_name,
        p.created_at,
        p.updated_at,
        p.created_by,
        p.created_by_atlas_id,
        '{{ var('agency_id') }}' AS agency_id,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        p.location_locality
    FROM duplicated_identities p
)

SELECT * FROM generated_people
