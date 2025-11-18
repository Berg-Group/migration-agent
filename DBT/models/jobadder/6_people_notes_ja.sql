{{ config(
    materialized='table',
    alias='person_notes_ja'
) }}

WITH base AS (
    SELECT noteid::text AS note_id, contactid::text AS person_id
    FROM {{ var('source_database') }}."contactnote"
    UNION ALL
    SELECT noteid::text AS note_id, contactid::text AS person_id
    FROM {{ var('source_database') }}."candidatenote"
),
links AS (
    SELECT
        note_id,
        person_id,
        ROW_NUMBER() OVER (PARTITION BY note_id, person_id ORDER BY person_id) AS rnk_pair
    FROM base
),
numbered AS (
    SELECT
        note_id,
        person_id,
        ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY person_id) AS rn
    FROM links
    WHERE rnk_pair = 1
),
note_details AS (
    SELECT
        nb.note_id,
        nb.person_id,
        nb.note_id || '_' || nb.rn AS unique_note_id,
        nb.note_id || COALESCE(nb.person_id,'') || '{{ var("clientName") }}' AS uuid_input,
        nt.createdbyuserid::text AS created_by_id,
        to_char(nt.datecreated, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(nt.datecreated, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        CASE nt.type WHEN 'Phone call' THEN 'phone_call' ELSE 'manual' END AS type,
        {{ html_to_markdown('nt.text') }} AS text
    FROM numbered nb
    LEFT JOIN {{ var('source_database') }}."note" nt ON nt.noteid::text = nb.note_id
),
user_lookup AS (
    SELECT id::text AS user_id, atlas_id::text AS user_atlas_id
    FROM {{ ref('users_ja') }}
),
atlas_mapping AS (
    SELECT id::text AS person_id, atlas_id::text AS atlas_person_id
    FROM {{ ref('1_people_ja') }}
)
SELECT
    nd.unique_note_id AS id,
    {{ atlas_uuid('nd.uuid_input') }} AS atlas_id,
    nd.person_id,
    am.atlas_person_id,
    nd.created_at,
    nd.updated_at,
    nd.created_by_id,
    COALESCE(ul.user_atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    nd.type,
    nd.text,
    nd.note_id AS source_note_id
FROM note_details AS nd
JOIN atlas_mapping am USING (person_id)
LEFT JOIN user_lookup ul ON nd.created_by_id = ul.user_id
WHERE nd.text IS NOT NULL
  AND TRIM(nd.text) <> ''
