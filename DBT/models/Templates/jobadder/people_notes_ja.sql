-- File: models/people_notes_ja.sql
{{ config(
    materialized='table',
    alias='people_notes_jobadder'
) }}

WITH note_details AS (
    SELECT
        candidatenote.noteid::TEXT AS id,
        candidatenote.contactid::TEXT       AS person_id,
        -- 1) created_by_id from source column
        note.createdbyuserid::TEXT          AS created_by_id,
        
        LEFT(note.datecreated::TEXT, 10) || 'T' ||
        SUBSTRING(note.datecreated::TEXT, 12, 8) AS created_at,
        
        LEFT(note.datecreated::TEXT, 10) || 'T' ||
        SUBSTRING(note.datecreated::TEXT, 12, 8)  AS updated_at,

        CASE
            WHEN note.type = 'Phone call' THEN 'phone'
            ELSE 'manual'
        END AS type,

        -- Remove all HTML, then replace &nbsp; with a normal space
        REGEXP_REPLACE(
          REGEXP_REPLACE(note.text, '<[^>]*>', ''),
          '&nbsp;',
          ' '
        ) AS text
    FROM {{ var('source_database') }}."contactnote" AS candidatenote
    LEFT JOIN {{ var('source_database') }}."note" AS note
        ON candidatenote.noteid = note.noteid
),

-- 2) Pull user IDs + atlas IDs from users_ja
user_lookup AS (
    SELECT
        id::TEXT     AS user_id,
        atlas_id::TEXT AS user_atlas_id
    FROM {{ ref('users_ja')}}
),

atlas_mapping AS (
    SELECT
        people_ja.id::TEXT       AS person_id,
        people_ja.atlas_id::TEXT AS atlas_person_id
    FROM {{ ref('1_people_ja')}} AS people_ja
)

SELECT
    nd.id AS id,
    nd.id AS atlas_id,  -- atlas_id is the same as id
    nd.person_id,
    am.atlas_person_id,
    nd.created_at,
    nd.updated_at,
    
    -- created_by_id from note_details
    nd.created_by_id,

    -- 3) created_by_atlas_id from user_lookup; fallback to master_id
    COALESCE(ul.user_atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,

    nd.type,
    nd.text
FROM note_details nd
INNER JOIN atlas_mapping am
       ON nd.person_id = am.person_id
LEFT JOIN user_lookup ul
       ON nd.created_by_id = ul.user_id
WHERE nd.text IS NOT NULL
  AND TRIM(nd.text) != ''