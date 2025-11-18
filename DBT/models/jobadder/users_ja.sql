-- models/users_ja.sql
{{ config(materialized='table', alias='users_ja') }}

WITH source AS (
    SELECT
        s.datecreated              AS created_at,
        s.lastlogindate           AS updated_at,
        s.email,
        s.displayname             AS name,
        s.userid                  AS id,
        COALESCE(
            um.atlas_id,
            LOWER(
                SUBSTRING(MD5('{{ var("clientName") }}' || s.userid::text), 1, 8) || '-' ||
                SUBSTRING(MD5('{{ var("clientName") }}' || s.userid::text), 9, 4) || '-' ||
                SUBSTRING(MD5('{{ var("clientName") }}' || s.userid::text), 13, 4) || '-' ||
                SUBSTRING(MD5('{{ var("clientName") }}' || s.userid::text), 17, 4) || '-' ||
                SUBSTRING(MD5('{{ var("clientName") }}' || s.userid::text), 21, 12)
            )
        )                           AS atlas_id,
        'active'                   AS status
    FROM {{ var("source_database") }}.user AS s
    LEFT JOIN {{ ref('user_mapping') }} AS um  -- <-- Changed here
        ON TRIM(LOWER(s.email)) = TRIM(LOWER(um.email))
)

SELECT
    created_at,
    updated_at,
    email,
    name,
    id,
    atlas_id,
    status
FROM source
