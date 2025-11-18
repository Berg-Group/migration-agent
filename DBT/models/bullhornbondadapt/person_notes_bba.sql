{{ config(
    materialized='table',
    alias='person_notes_bba',
    tags=['bba']
) }}

WITH people_map AS (
  SELECT
      id::text AS person_id,
      atlas_id AS atlas_person_id
  FROM {{ ref('people_bba') }}
),
entries AS (
  SELECT
      id::bigint        AS entry_id,
      creation_date,
      notes_edit_date,
      creator_id,
      j_notes,
      j_document
  FROM {{ var('source_database') }}."journal_entries"
),
links AS (
  SELECT
      journal_id::bigint AS entry_id,
      entity_id::text    AS entity_id
  FROM {{ var('source_database') }}."lk_entities_journal"
),
linked_people AS (
  SELECT
      l.entry_id,
      pm.person_id,
      pm.atlas_person_id
  FROM links l
  JOIN people_map pm
    ON pm.person_id = l.entity_id
),
notes_raw AS (
  SELECT
      e.entry_id,
      e.creation_date,
      e.notes_edit_date,
      e.creator_id,
      COALESCE(NULLIF(BTRIM(e.j_notes), ''), NULLIF(BTRIM(e.j_document), '')) AS note_text
  FROM entries e
),
notes_filtered AS (
  SELECT
      entry_id,
      creation_date,
      notes_edit_date,
      creator_id,
      note_text
  FROM notes_raw
  WHERE note_text IS NOT NULL
),
processed AS (
  SELECT
      (lp.person_id || '_' || nf.entry_id::text)                       AS id,
      {{ atlas_uuid("lp.person_id || '_' || nf.entry_id::text") }}     AS atlas_id,
      TO_CHAR(nf.creation_date::timestamp, 'YYYY-MM-DD\"T\"00:00:00')  AS created_at,
      TO_CHAR(COALESCE(nf.notes_edit_date, nf.creation_date)::timestamp, 'YYYY-MM-DD\"T\"00:00:00') AS updated_at,
      {{ clean_html('nf.note_text') }}                                  AS text,
      lp.person_id                                                      AS person_id,
      lp.atlas_person_id                                                AS atlas_person_id,
      nf.creator_id::text                                               AS created_by_id,
      '{{ var("created_by_id") }}'                                      AS created_by_atlas_id,
      'manual'                                                          AS type
  FROM notes_filtered nf
  INNER JOIN linked_people lp
    ON lp.entry_id = nf.entry_id
)

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    text,
    person_id,
    atlas_person_id,
    created_by_id,
    created_by_atlas_id,
    type
FROM processed
