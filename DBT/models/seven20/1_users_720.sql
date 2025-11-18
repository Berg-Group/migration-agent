{{ config(
    materialized='table',
    alias='1_users_720',
    tags=["seven20"]
) }}

with internal_users AS (
    SELECT
        atlas_id,
        email,
        name
    FROM {{ this.schema }}.atlas_users
)
SELECT
    u.id,
    COALESCE(iu.atlas_id, {{ atlas_uuid('u.id || u.email') }}) AS atlas_id,
    COALESCE(iu.name, u.firstname || ' ' || u.lastname) AS name,
    COALESCE(iu.email, u.email) AS email,
    CASE 
        WHEN isactive = 1 THEN 'active' 
        ELSE 'disabled' 
    END AS status
FROM 
    {{ var('source_database') }}."user" u
LEFT JOIN 
    internal_users iu ON LOWER(iu.email) = LOWER(u.email)
WHERE 
    u.email ILIKE '%{{var('domain')}}%'