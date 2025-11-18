{{ config(
    materialized='table',
    alias='people_loxo_csv'
) }}

    SELECT
        c.id::varchar AS id,
        lower(
            substring(md5('{{ var('clientName') }}' || c.id::text || c.email), 1, 8) || '-' ||
            substring(md5('{{ var('clientName') }}' || c.id::text || c.email), 9, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || c.id::text || c.email), 13, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || c.id::text || c.email), 17, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || c.id::text || c.email), 21, 12)
        ) AS atlas_id,
        split_part(name, ' ', 1) AS first_name,
        substring(name from position(' ' in name) + 1) AS last_name,
        '{{ var('created_by_id') }}' AS created_by_id,
        '{{ var('created_by_id') }}'  AS updated_by_id,
        '{{ var('agency_id') }}' AS agency_id,
        TO_CHAR(created_date::DATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(recent_activity_date::DATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        c.location AS location_locality
    FROM {{ var('source_database') }}."candidate" c


UNION ALL 


    SELECT 
        cl.id::varchar AS id,
        lower(
            substring(md5('{{ var('clientName') }}' || cl.id::text || cl.email), 1, 8) || '-' ||
            substring(md5('{{ var('clientName') }}' || cl.id::text || cl.email), 9, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || cl.id::text || cl.email), 13, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || cl.id::text || cl.email), 17, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || cl.id::text || cl.email), 21, 12)
        ) AS atlas_id,
        split_part(cl.name, ' ', 1) AS first_name,
        substring(cl.name from position(' ' in name) + 1) AS last_name, 
        '{{ var('created_by_id') }}' AS created_by_id,
        '{{ var('created_by_id') }}'  AS updated_by_id,
        '{{ var('agency_id') }}' AS agency_id,
        TO_CHAR(cl.created_date::DATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(cl.recent_activity_date::DATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        cl.location AS location_locality 
    FROM 
        {{ var('source_database') }}."client" cl