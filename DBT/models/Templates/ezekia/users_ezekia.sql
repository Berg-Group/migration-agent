{{ config(
    materialized='table',
    alias='users_ezekia'
) }}

WITH source_data AS (
    SELECT
        u.id,
        u.fullname AS name,
        email,
        u.role
    FROM {{ var("source_database") }}.users AS u
    WHERE u.role = 'search'
    AND split_part(email, '@', 2) ILIKE '{{ var("domain") }}'

)


    SELECT
        sd.id,
        COALESCE(up.id, {{atlas_uuid('sd.id')}} ) AS atlas_id,
        COALESCE(up.name, sd.name) AS name,
        COALESCE(up.email, sd.email) AS email,
        COALESCE(up.status, 'disabled') AS status
    FROM source_data sd
    INNER JOIN {{this.schema}}."users_prod" up ON LOWER(TRIM(up.email)) = LOWER(TRIM(sd.email))
