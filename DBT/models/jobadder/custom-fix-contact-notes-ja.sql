-- File: models/fix_contact_notes_ja.sql
{{ config(
    materialized='table',
    alias='fix_contact_notes_ja'
) }}

WITH note_details AS (
    SELECT
        -- Use noteid as the unique ID
        n.noteid::TEXT AS id,
        
        -- Link to person (contact)
        cn.contactid::TEXT AS person_id,
        
        -- Who created it
        n.createdbyuserid::TEXT AS created_by_id,
        
        -- Created/updated timestamps in ISO-like format
        LEFT(n.datecreated::TEXT, 10) || 'T' ||
        SUBSTRING(n.datecreated::TEXT, 12, 8) || 'Z' AS created_at,

        LEFT(n.datecreated::TEXT, 10) || 'T' ||
        SUBSTRING(n.datecreated::TEXT, 12, 8) || 'Z' AS updated_at,

        -- Example "type" logic if relevant, or leave static
        CASE
            WHEN n.source = 'Note' THEN 'manual'
            ELSE 'manual'
        END AS type,

        -- Clean the note text
        REGEXP_REPLACE(
            REGEXP_REPLACE(n.text, '<[^>]*>', ''),
            '&nbsp;',
            ' '
        ) AS text
    FROM {{ var('source_database') }}."note" n
    JOIN {{ var('source_database') }}."contactnote" cn
        ON n.noteid = cn.noteid
    JOIN {{ var('source_database') }}."contact" c
        ON cn.contactid = c.contactid
    WHERE n.summary IS NOT NULL
      AND n.source = 'Note'
),

-- 1) Map user IDs to atlas IDs
user_lookup AS (
    SELECT
        id::TEXT       AS user_id,
        atlas_id::TEXT AS user_atlas_id
    FROM {{ ref('users_ja') }}
),

-- 2) Map person (contact) IDs to atlas IDs
atlas_mapping AS (
    SELECT
        p.id::TEXT      AS person_id,
        p.atlas_id::TEXT AS atlas_person_id
    FROM {{ ref('1_people_ja') }} p
)

SELECT
    nd.id AS id,
    nd.id AS atlas_id,  -- Often, atlas_id is the same as the record’s source ID
    nd.person_id,
    am.atlas_person_id,
    nd.created_at,
    nd.updated_at,
    
    -- Created by
    nd.created_by_id,
    COALESCE(ul.user_atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,

    nd.type,
    nd.text
FROM note_details nd
LEFT JOIN atlas_mapping am
       ON nd.person_id = am.person_id
LEFT JOIN user_lookup ul
       ON nd.created_by_id = ul.user_id
WHERE nd.text IS NOT NULL
  AND TRIM(nd.text) != ''
