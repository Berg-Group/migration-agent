{{ config(
    materialized='table',
    alias='company_notes_ezekia'
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ref('companies_ezekia')}}
)


    SELECT
        s.client_id::TEXT || '_note' AS id,
        {{atlas_uuid('s.client_id || s.speciality')}} AS atlas_id,
        TO_CHAR(s.created_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(s.updated_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        {{clean_html('s.speciality')}} AS text,
        'manual' AS type,
        ic.company_id,
        ic.atlas_company_id,
        coalesce(user_id, '1') AS created_by_id,
        coalesce(user_id, '1') AS updated_by_id,
        COALESCE(u.atlas_id, '{{var("master_id")}}') AS created_by_atlas_id,
        COALESCE(u.atlas_id, '{{var("master_id")}}') AS updated_by_atlas_id
    FROM {{ var("source_database") }}.search_firms_clients s
    LEFT JOIN {{ref('users_ezekia')}} u ON u.id = s.user_id
    INNER JOIN internal_companies ic ON ic.company_id = s.client_id
    WHERE NULLIF(TRIM(s.speciality), '') NOTNULL
