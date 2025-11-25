{{ config(
    materialized='table',
    alias='companies_ezekia'
) }}

WITH base AS (
    SELECT
        s.client_id AS id,
        {{atlas_uuid('s.client_id')}} AS atlas_id,
        TO_CHAR(s.created_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(s.updated_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        s.name,
        s.description AS summary,
        SPLIT_PART(REPLACE(s.size, ' employees', ''), '-', 1)::varchar AS size,
        CASE 
            WHEN s.label = 'client' THEN 'client'
            ELSE 'none'
        END AS relationship,
        '{{var("master_id")}}' AS created_by_atlas_id,
        '{{var("master_id")}}' AS updated_by_atlas_id 
    FROM {{ var("source_database") }}.search_firms_clients s
)

SELECT 
    id,
    atlas_id,
    created_at,
    updated_at,
    name,
    summary,
    {{number_range('size')}} AS company_size,
    relationship,
    created_by_atlas_id,
    updated_by_atlas_id
FROM base
