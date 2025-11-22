{{ config(
    materialized='table',
    alias='users_vin'
) }}


SELECT
    pu.id,
    COALESCE(
        um.id,
        {{ atlas_uuid("pu.email || pu.name") }}
    ) AS atlas_id,
    COALESCE(um.name, pu.name) AS name,
    CASE WHEN COALESCE(um.email, pu.email) ilike '%@%' THEN COALESCE(um.email, pu.email)
        ELSE LOWER(REGEXP_REPLACE(pu.name, ' ', '.')) || '@' || '{{var("domain")}}' END AS email,
    COALESCE(um.status, 'active') as status
FROM 
    {{ var('source_database') }}."public_user_account" pu
LEFT JOIN {{ref('user_mapping')}} um USING (email)
WHERE pu.deleted_timestamp ISNULL 
AND pu.email NOT LIKE '%vincere%'