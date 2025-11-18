{{ config(
    materialized = 'table',
    alias        = 'file_person_mapping_bh',
    tags         = ['bullhorn']
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM 
        {{ ref('1_people_bh') }}
)
SELECT 
    w.userid AS person_id,
    ip.atlas_person_id,
    w."name" AS actual_file_name,
    CASE
        WHEN w."type" ILIKE '%resume%' OR w."type" ILIKE '%cv%' THEN 'resume'
        ELSE 'other'
    END AS type,
    REPLACE('people/' || f.directory || f.userworkfileid::VARCHAR || f.fileextension, '\\', '/') AS file_name
FROM {{ var('source_database') }}.bh_userworkfile f
INNER JOIN {{ var('source_database') }}.bh_userwork w ON w.userworkid = f.userworkid 
INNER JOIN internal_persons ip ON ip.person_id = w.userid