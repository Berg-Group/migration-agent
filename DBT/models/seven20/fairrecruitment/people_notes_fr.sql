{{ config(
    materialized='table',
    alias='person_notes_fr',
    tags=["seven20"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM {{ref('people_fr')}}
)

SELECT 
    t.id,
    {{atlas_uuid('t.id || ip.person_id || t.description')}} AS atlas_id,
    {{clean_html('t.description')}} AS text, 
    CASE 
        WHEN t.subject = 'Call' THEN 'phone_call' 
        ELSE 'manual' 
    END AS type,
    to_char(t.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    to_char(t.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    t.createdbyid AS created_by_id, 
    t.lastmodifiedbyid AS updated_by_id,
    COALESCE(u.atlas_id, '{{var('master_id')}}') AS created_by_atlas_id,
    COALESCE(u2.atlas_id, '{{var('master_id')}}') AS updated_by_atlas_id,
    ip.person_id,
    ip.atlas_person_id
FROM {{ var('source_database') }}."task" t
INNER JOIN internal_persons ip ON ip.person_id = t.whoid
LEFT JOIN {{ref('1_users_720')}} u ON u.id = t.createdbyid
LEFT JOIN {{ref('1_users_720')}} u2 ON u2.id = t.lastmodifiedbyid
WHERE ip.atlas_person_id IS NOT NULL
    AND TRIM(t.description) <> '' AND t.description NOTNULL