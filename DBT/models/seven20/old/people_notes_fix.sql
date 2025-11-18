{{ config(
    materialized='table',
    alias='sjt_people_notes_fix',
    tags=["seven20"]
) }}

WITH internal_persons AS (
    SELECT 
        person_id,
        atlas_id AS atlas_person_id 
    FROM "{{ this.schema }}"."sjt_people_fix"
)

SELECT 
    t.id,
    lower(
        substring(md5(t.id::text || current_date::text), 1, 8) || '-' ||
        substring(md5(t.id::text || current_date::text), 9, 4) || '-' ||
        substring(md5(t.id::text || current_date::text), 13, 4) || '-' ||
        substring(md5(t.id::text || current_date::text), 17, 4) || '-' ||
        substring(md5(t.id::text || current_date::text), 21, 12)
    ) AS atlas_id,
    t.description AS text, 
    CASE 
        WHEN t.subject = 'Call' THEN 'phone_call' 
        ELSE 'manual' 
    END AS type,
    to_char(t.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    u.atlas_id AS created_by_id,
    ip.person_id,
    ip.atlas_person_id
FROM {{ var('source_database') }}."task" t
INNER JOIN internal_persons ip ON ip.person_id = t.whoid
LEFT JOIN "{{ this.schema }}"."users" u ON u.id = t.createdbyid
WHERE ip.atlas_person_id IS NOT NULL