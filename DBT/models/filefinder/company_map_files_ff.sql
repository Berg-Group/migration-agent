{{ config(
    materialized='table',
    alias='company_map_files_ff',
    tags=['filefinder']
) }}

WITH documents AS (
    SELECT 
        c.id AS company_id,
        c.atlas_id AS atlas_company_id,
        (UPPER(SUBSTRING(d.newdocumentname, 1, 2)) || '/' || d.newdocumentname) AS file_name,
        d.originaldocumentname AS actual_file_name
    FROM {{ var('source_database') }}.document d 
    INNER JOIN {{ var('source_database') }}.entitydocument e ON e.iddocument = d.iddocument 
    INNER JOIN {{ ref('4_companies_ff') }} c ON c.id = e.entityid 
    WHERE NULLIF(TRIM(COALESCE(d.newdocumentname, '')), '') IS NOT NULL
      AND NULLIF(TRIM(COALESCE(d.originaldocumentname, '')), '') IS NOT NULL
      AND d.newdocumentname LIKE '%.%'
)
SELECT 
    company_id,
    atlas_company_id,
    file_name,
    actual_file_name
FROM documents