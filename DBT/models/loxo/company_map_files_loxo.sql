{{ config(
    materialized = 'table',
    alias        = 'company_map_files_loxo',
    tags         = ['loxo']
) }}

SELECT
    c.id AS company_id,
    c.atlas_id AS atlas_company_id,
    cd."path" AS file_name,
    cd.filename AS actual_file_name
FROM {{ var('source_database') }}.companies_documents cd
INNER JOIN {{ ref('3_companies_loxo') }} c ON c.id = cd.root_id
WHERE cd."path" != ''