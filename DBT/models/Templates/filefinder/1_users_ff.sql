{{ config(
    materialized='table',
    alias='users_ff',
    tags=["filefinder"]
) }}

WITH atlas_users AS (
    SELECT 
        atlas_id,
        email,
        name, 
        status
    FROM {{ this.schema }}.atlas_users
),
source_users AS (
    SELECT 
        iduser AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || iduser::text") }} AS atlas_id,
        TO_CHAR(createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        useremail AS email,
        fullname AS name
    FROM {{ var('source_database') }}.user
    WHERE useremail ILIKE '%@{{ var('domain') }}'
),
final_users AS (
SELECT 
    u.id,
    COALESCE(au.atlas_id, u.atlas_id) AS atlas_id,
    COALESCE(au.name, u.name) AS name,
    CASE
        WHEN au.status IS NOT NULL THEN au.status
        ELSE 'disabled'
    END AS status,
    u.email,
    u.created_at,
    u.updated_at,
    ROW_NUMBER() OVER (
        PARTITION BY LOWER(u.email)
        ORDER BY u.created_at ASC, u.id ASC
    ) AS rn
FROM source_users u
LEFT JOIN atlas_users au ON LOWER(u.email) = LOWER(au.email)
)
SELECT 
    id,
    atlas_id,
    name,
    status,
    email,
    created_at,
    updated_at
FROM final_users
WHERE rn = 1