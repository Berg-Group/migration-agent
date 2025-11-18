
{{ config(
    materialized='table',
    alias='company_contacts_loxo_csv',
    tags=["loxo"]
) }}

WITH internal_persons AS (
    SELECT 
        DISTINCT id AS person_id, 
                        atlas_id AS atlas_person_id
         FROM "{{ this.schema }}"."people_loxo"
)

,internal_companies AS (
    SELECT DISTINCT id AS company_id,
                    atlas_id AS atlas_company_id 
    FROM  "{{ this.schema }}"."companies_loxo"
)


    SELECT
        c.id::text AS person_id,
        atlas_person_id,
        lower(
            substring(md5(c.id::text || '-' || coalesce(c.title::text, 'NULL')), 1, 8) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(c.title::text, 'NULL')), 9, 4) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(c.title::text, 'NULL')), 13, 4) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(c.title::text, 'NULL')), 17, 4) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(c.title::text, 'NULL')), 21, 12)
        ) AS atlas_id,
        to_char(c.created_date::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(c.recent_activity_date::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        '{{ var('agency_id') }}' AS agency_id,
        ic.atlas_company_id,
        'prospect' AS role,
        c.title

    FROM 
        {{ var('source_database') }}."client" c 
    LEFT JOIN internal_persons ip ON ip.person_id = c.id 
    LEFT JOIN internal_companies ic ON ic.company_id = c.id 