{{ config(
    materialized='table',
    alias='users_ft',
    tags=["forgetalent"]
) }}

WITH ranked_users AS (
    SELECT 
        c.UserID AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || UserID::text") }} AS atlas_id,
        TO_CHAR(c.DateAdded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(c.DateLastModified::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        c.firstname || ' ' || c.lastname AS name,
        CASE 
            WHEN c.email LIKE '%@%' THEN
                REGEXP_REPLACE(
                    LOWER(TRIM(c.email)),
                    '^([^@]+)-([^@]+\\.[^@]+)$',
                    '\\1@\\2'
                )
            ELSE
                REGEXP_REPLACE(
                    LOWER(TRIM(c.email)),
                    '^([^-]+)-([^-]+.*)$',
                    '\\1@\\2'
                )
        END AS email,
        'active' AS status,
        ROW_NUMBER() OVER (
            PARTITION BY CASE 
                WHEN c.email LIKE '%@%' THEN
                    REGEXP_REPLACE(
                        LOWER(TRIM(c.email)),
                        '^([^@]+)-([^@]+\\.[^@]+)$',
                        '\\1@\\2'
                    )
                ELSE
                    REGEXP_REPLACE(
                        LOWER(TRIM(c.email)),
                        '^([^-]+)-([^-]+.*)$',
                        '\\1@\\2'
                    )
            END
            ORDER BY c.DateAdded
        ) AS row_number
    FROM {{ var("source_database") }}.bh_usercontact c
    WHERE LOWER(c.email) LIKE '%' || '{{ var('domain') }}' || '%'
),
internal_users AS (
    SELECT
        id AS atlas_id,
        email,
        name, 
        status
    FROM {{ this.schema }}.users_from_prod
)
SELECT 
    r.id,
    COALESCE(iu.atlas_id, r.atlas_id) AS atlas_id,
    COALESCE(iu.name, r.name) AS name,
    r.created_at,
    r.updated_at,
    COALESCE(iu.email, r.email) AS email,
    r.status AS status
FROM ranked_users r
LEFT JOIN internal_users iu ON LOWER(TRIM(r.email)) = LOWER(TRIM(iu.email))
WHERE r.row_number = 1