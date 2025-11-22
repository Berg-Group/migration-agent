{{ config(
    materialized='table',
    alias='people_notes_720',
    tags=["seven20"]
) }}

with internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM 
         "{{ this.schema }}"."people"
)

SELECT 
    t.id,
    lower(
            substring(md5(t.id::text), 1, 8) || '-' ||
            substring(md5(t.id::text), 9, 4) || '-' ||
            substring(md5(t.id::text), 13, 4) || '-' ||
            substring(md5(t.id::text), 17, 4) || '-' ||
            substring(md5(t.id::text), 21, 12)
        ) AS atlas_id, 
    t.description AS text, 
    CASE WHEN t.subject = 'Call' THEN 'phone_call' ELSE 'manual' END AS type,
    to_char(t.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    u.atlas_id AS created_by_id,
    ip.person_id,
    ip.atlas_person_id
FROM 
    {{ var('source_database') }}."task" t
LEFT JOIN 
    internal_persons ip ON ip.person_id = t.whoid
LEFT JOIN 
    "{{ this.schema }}"."users" u ON u.id = t.createdbyid
WHERE ip.atlas_person_id NOTNULL