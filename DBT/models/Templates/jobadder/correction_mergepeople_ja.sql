-- note I unmerged everyone on the DB so this script remerges them
{{ config(
    materialized='table',
    alias='correction_mergepeople'
) }}

WITH source AS (
    SELECT
        c.previouscontactid  AS original_person_id,
        c.contactid          AS merge_person_id,
        c.linkedinurl        AS linkedin_url,
        c.email              AS email
    FROM {{ var('source_database') }}."contact" AS c
    WHERE c.previouscontactid IS NOT NULL
),

people_lookup AS (
    SELECT
        p.id       AS person_id,
        p.atlas_id AS person_atlas_id
    FROM {{ ref('1_people_ja') }} p
)

SELECT
    s.original_person_id,
    s.merge_person_id,
    -- Join for atlas IDs
    pl_original.person_atlas_id AS atlas_original_person_id,
    pl_merge.person_atlas_id    AS atlas_merge_person_id,
    s.linkedin_url,
    s.email
FROM source AS s
-- Join to get atlas ID for original_person_id
LEFT JOIN people_lookup pl_original
       ON s.original_person_id = pl_original.person_id
-- Join to get atlas ID for merge_person_id
LEFT JOIN people_lookup pl_merge
       ON s.merge_person_id = pl_merge.person_id
