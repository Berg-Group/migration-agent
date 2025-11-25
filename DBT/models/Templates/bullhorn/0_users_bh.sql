{{ config(
    materialized = 'table',
    alias        = 'users_bh',
    tags         = ['bullhorn']
) }}

WITH ranked_users AS (
    SELECT 
        c.UserID AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || UserID::text") }} AS atlas_id,
        TO_CHAR(c.DateAdded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(c.DateLastModified::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
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
            ORDER BY c.DateAdded DESC
        ) AS row_number
    FROM {{ var("source_database") }}.bh_usercontact c
    WHERE LOWER(c.email) LIKE '%' || '{{ var('domain') }}' || '%'
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
    COALESCE(NULLIF(TRIM(iu.name), ''), NULLIF(TRIM(r.name), ''), r.email) AS name,
    r.created_at,
    r.updated_at,
    r.email,
    CASE 
        WHEN iu.status IS NOT NULL THEN iu.status
        ELSE 'disabled'
    END AS status
FROM ranked_users r
LEFT JOIN internal_users iu ON LOWER(r.email) = LOWER(iu.email)
WHERE r.row_number = 1