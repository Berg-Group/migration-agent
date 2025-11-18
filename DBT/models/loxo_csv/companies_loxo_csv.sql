{{ config(
    materialized='table',
    alias='companies_loxo_csv',
    tags=["loxo"]
) }}

    SELECT
        id,
    LOWER(
        SUBSTRING(md5(id::text || company), 1, 8) || '-' ||
        SUBSTRING(md5(id::text || company), 9, 4) || '-' ||
        SUBSTRING(md5(id::text || company), 13, 4) || '-' ||
        SUBSTRING(md5(id::text || company), 17, 4) || '-' ||
        SUBSTRING(md5(id::text || company), 21, 12)
        ) AS atlas_id,
        company AS name,
        to_char(created_date::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        location AS location_locality,
        '{{ var('agency_id') }}' AS agency_id,
        'target' AS relationship
    FROM 
        {{ var('source_database') }}."candidate"

UNION ALL 

    SELECT 
        id,
    LOWER(
        SUBSTRING(md5(id::text || company), 1, 8) || '-' ||
        SUBSTRING(md5(id::text || company), 9, 4) || '-' ||
        SUBSTRING(md5(id::text || company), 13, 4) || '-' ||
        SUBSTRING(md5(id::text || company), 17, 4) || '-' ||
        SUBSTRING(md5(id::text || company), 21, 12)
        ) AS atlas_id,
        company AS name,
        to_char(created_date::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        location AS location_locality,
        '{{ var('agency_id') }}' AS agency_id,
        'client' AS relationship
    FROM 
        {{ var('source_database') }}."client"  
