{{ config(
    materialized='table',
    alias='file_person_mapping_ff',
    tags=['filefinder']
) }}

WITH documents AS (
    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        (UPPER(SUBSTRING(d.newdocumentname, 1, 2)) || '/' || d.newdocumentname) AS file_name,
        d.originaldocumentname AS actual_file_name,
        CASE
            WHEN LOWER(COALESCE(d.originaldocumentname, '')) LIKE '%resume%'
              OR LOWER(COALESCE(d.originaldocumentname, '')) LIKE '%cv%'
              OR LOWER(COALESCE(d.originaldocumentname, '')) LIKE '%profile%'
              OR LOWER(COALESCE(d.description, '')) LIKE '%resume%'
              OR LOWER(COALESCE(d.description, '')) LIKE '%cv%'
              OR LOWER(COALESCE(d.description, '')) LIKE '%profile%'
            THEN 'resume'
            ELSE 'other'
        END AS type
    FROM {{ var('source_database') }}.document d 
    INNER JOIN {{ var('source_database') }}.entitydocument e ON e.iddocument = d.iddocument 
    INNER JOIN {{ ref('2_people_ff') }} p ON p.id = e.entityid
    WHERE NULLIF(TRIM(COALESCE(d.newdocumentname, '')), '') IS NOT NULL
      AND NULLIF(TRIM(COALESCE(d.originaldocumentname, '')), '') IS NOT NULL
      AND d.newdocumentname LIKE '%.%'
)
SELECT 
    person_id,
    atlas_person_id,
    file_name,
    actual_file_name,
    type
FROM documents