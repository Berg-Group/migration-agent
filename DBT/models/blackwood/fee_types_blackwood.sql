{{ config(
    materialized='table',
    alias='fee_types_blackwood',
    tags=["blackwood"]
) }}

SELECT 
    DISTINCT COALESCE(NULLIF(lower(trim(retainer_type::text)), ''), 'unknown_fee') AS name,
    'fee' AS project_fee_type,
    {{ atlas_uuid('lower(trim(retainer_type::text))') }} AS atlas_id,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00') AS created_at,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
    '{{var('agency_id')}}' AS agency_id,
    '{{var('created_by_id')}}' AS created_by_atlas_id
FROM 
    {{var('source_database')}}."financial_retainer"