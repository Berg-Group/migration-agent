{{ config(
    materialized='table',
    alias='users_rect',
    tags=['recruitly']
) }}

WITH ranked_users AS (
    SELECT 
        u.user_id AS id,
        {{ atlas_uuid('u.user_id') }} AS atlas_id,
        {{ string_to_timestamp('u.createdon', 'YYYY-MM-DD"T00:00:00Z"') }} AS created_at,
        {{ string_to_timestamp('u.modifiedon', 'YYYY-MM-DD"T"HH24:MI:SS') }} AS updated_at,
        COALESCE(NULLIF(TRIM(u.first_name), ''), '') || ' ' || COALESCE(NULLIF(TRIM(u.surname), ''), '') AS name,
        {{ email_norm('u.email') }} AS email,
        CASE 
            WHEN LOWER(COALESCE(u.is_disabled::varchar, 'false')) = 'true' THEN 'disabled'
            ELSE 'active'
        END AS source_status,
        ROW_NUMBER() OVER (
            PARTITION BY {{ email_norm('u.email') }}
            ORDER BY u.createdon DESC
        ) AS row_number
    FROM {{ var('source_database') }}.users u
    WHERE LOWER({{ email_norm('u.email') }}) LIKE '%' || '{{ var('domain') }}' || '%'
),
internal_users AS (
    SELECT
        atlas_id,
        email,
        name, 
        status
    FROM {{ this.schema }}.atlas_users
)
SELECT 
    r.id,
    CASE
        WHEN iu.atlas_id IS NOT NULL THEN iu.atlas_id
        ELSE r.atlas_id
    END AS atlas_id,
    CASE
        WHEN iu.name IS NOT NULL THEN iu.name
        ELSE r.name
    END AS name,
    r.created_at,
    r.updated_at,
    r.email,
    COALESCE(iu.status, r.source_status) AS status
FROM ranked_users r
LEFT JOIN internal_users iu ON LOWER(r.email) = LOWER(iu.email)
WHERE r.row_number = 1