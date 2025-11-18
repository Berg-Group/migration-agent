{{ config(
    materialized = 'table',
    alias = 'person_notes_invenias',
    tags=["invenias"]
) }}

WITH internal_persons AS (
SELECT 
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM 
    {{ref('people_invenias')}}
),

merged AS (SELECT 
    p.itemid AS id,
    {{atlas_uuid('itemid || ip.person_id')}} AS atlas_id,
    {{clean_html('p.content')}} AS text,
    to_char(p.telephoneactiondate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(p."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    p.creatorid AS created_by_id,
    COALESCE(u.atlas_id, '{{var("master_id")}}') AS created_by_atlas_id,
    'phone_call' AS type,
    ip.person_id,
    ip.atlas_person_id
FROM 
    {{ var('source_database') }}."telephones" p 
LEFT JOIN  {{ var('source_database') }}."relation_persontotelephone" pn ON pn.telephoneid = p.itemid
INNER JOIN internal_persons ip ON ip.person_id = pn.personid
LEFT JOIN 
    {{ref('users_invenias')}} u ON u.id = p.creatorid 

UNION ALL 

SELECT 
    e.itemid AS id,
    {{atlas_uuid('itemid || ip.person_id')}} AS atlas_id,
    {{clean_html('e.body')}} AS text,
    to_char(e.datecreated::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(e."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    e.creatorid AS created_by_id,
    COALESCE(u.atlas_id, '{{var("master_id")}}') AS created_by_atlas_id,
    'manual' AS type,
    ip.person_id,
    ip.atlas_person_id
FROM 
    {{ var('source_database') }}."emails" e 
LEFT JOIN  {{ var('source_database') }}."relation_persontoemail" pe ON pe.emailid = e.itemid
INNER JOIN internal_persons ip ON ip.person_id = pe.personid
LEFT JOIN 
   {{ref('users_invenias')}} u ON u.id = e.creatorid 
UNION ALL 


SELECT 
    p.itemid AS id,
    {{atlas_uuid('itemid || ip.person_id')}} AS atlas_id,
    {{clean_html('p.content')}} AS text,
    to_char(p.datecreated::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    TO_CHAR(p."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        p.creatorid AS created_by_id,
    COALESCE(u.atlas_id, '{{var("master_id")}}') AS created_by_atlas_id,
    CASE WHEN noteactiontype = '4693FEE3-6725-4E0C-8370-5244EB2F04DB' THEN 'phone_call' ELSE 'manual' END AS type,
    ip.person_id,
    ip.atlas_person_id
FROM 
    {{ var('source_database') }}."notes" p 
LEFT JOIN  {{ var('source_database') }}."relation_persontonote" pn ON pn.noteid = p.itemid
INNER JOIN internal_persons ip ON ip.person_id = pn.personid
LEFT JOIN 
     {{ref('users_invenias')}} u ON u.id = p.creatorid )


SELECT * FROM merged
WHERE text <> '...' AND NULLIF(TRIM(text), '') IS NOT NULL  