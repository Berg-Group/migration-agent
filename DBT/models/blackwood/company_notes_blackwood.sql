{{ config(materialized='table', alias='company_notes_blackwood', tags=['blackwood']) }}

WITH notes_src AS (
    SELECT
        CONCAT('l2-', n.level2companyid)      AS original_company_id,
        TRIM(n.company_marketingnotes)        AS note_text
    FROM {{ var('source_database') }}.level2company_notes n
    WHERE n.company_marketingnotes IS NOT NULL
      AND TRIM(n.company_marketingnotes) <> ''
),

mapped AS (
    SELECT
        COALESCE(m.new_company_id, n.original_company_id) AS company_id,
        n.note_text
    FROM notes_src n
    LEFT JOIN {{ ref('companies_mapping_blackwood') }} m
           ON m.original_company_id = n.original_company_id
),

dedup AS (
    SELECT
        company_id,
        note_text,
        ROW_NUMBER() OVER (PARTITION BY company_id, note_text) AS rn
    FROM mapped
),

companies AS (
    SELECT id, atlas_id
    FROM {{ ref('companies_blackwood') }}
)

SELECT
    LOWER(
        SUBSTRING(md5(companies.atlas_id || dedup.note_text),  1,  8) || '-' ||
        SUBSTRING(md5(companies.atlas_id || dedup.note_text),  9,  4) || '-' ||
        SUBSTRING(md5(companies.atlas_id || dedup.note_text), 13,  4) || '-' ||
        SUBSTRING(md5(companies.atlas_id || dedup.note_text), 17,  4) || '-' ||
        SUBSTRING(md5(companies.atlas_id || dedup.note_text), 21, 12)
    )                                           AS atlas_id,
    companies.id                                AS company_id,
    companies.atlas_id                          AS atlas_company_id,
    dedup.note_text                             AS text,
    'manual'                                    AS type,
    '1'                                         AS created_by_id,
    '{{ var("master_id") }}'                    AS created_by_atlas_id,
    '{{ var("agency_id") }}'                    AS agency_id,
    TO_CHAR(CURRENT_DATE,'YYYY-MM-DD"T"00:00:00') AS created_at,
    TO_CHAR(CURRENT_DATE,'YYYY-MM-DD"T"00:00:00') AS updated_at
FROM dedup
JOIN companies ON companies.id = dedup.company_id
WHERE dedup.rn = 1