{{ config(
    materialized = 'table',
    alias = 'people_notes_invenias',
    tags=["invenias"]
) }}

WITH internal_persons AS (
SELECT 
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM 
    "{{this.schema}}"."people_invenias"
)

SELECT 
    p.itemid AS id,
        lower(
        substring(md5(p."itemid"::text), 9, 4) || '-' ||
        substring(md5(p."itemid"::text), 1, 8) || '-' ||
        substring(md5(p."itemid"::text), 13, 4) || '-' ||
        substring(md5(p."itemid"::text), 17, 4) || '-' ||
        substring(md5(p."itemid"::text), 21, 12)
    ) AS atlas_id,
    p.content AS text,
    to_char(p.datecreated::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    u.atlas_id AS created_by_id,
    CASE WHEN noteactiontype = '4693FEE3-6725-4E0C-8370-5244EB2F04DB' THEN 'phone_call' ELSE 'manual' END AS type,
    ip.person_id,
    ip.atlas_person_id
FROM 
    {{ var('source_database') }}."notes_clean" p 
LEFT JOIN  {{ var('source_database') }}."relation_persontonote" pn ON pn.noteid = p.itemid
LEFT JOIN internal_persons ip ON ip.person_id = pn.personid
LEFT JOIN 
    "{{this.schema}}"."users_invenias" u ON u.id = p.creatorid 