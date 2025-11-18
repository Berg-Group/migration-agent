
{{ config(
    materialized='table',
    alias='company_contacts_720',
    tags=["seven20"]
) }}

WITH internal_persons AS (
    SELECT 
        DISTINCT id AS person_id, 
                        atlas_id AS atlas_person_id
         FROM "{{ this.schema }}"."people"
)

,internal_companies AS (
    SELECT DISTINCT id AS company_id,
                    atlas_id AS atlas_company_id 
    FROM  "{{ this.schema }}"."companies"
)

, base AS (
    SELECT
        c.id::text AS person_id,
        lower(
            substring(md5(c.id::text || '-' || coalesce(a.id::text, 'NULL')), 1, 8) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(a.id::text, 'NULL')), 9, 4) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(a.id::text, 'NULL')), 13, 4) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(a.id::text, 'NULL')), 17, 4) || '-' ||
            substring(md5(c.id::text || '-' || coalesce(a.id::text, 'NULL')), 21, 12)
        ) AS atlas_id,
        to_char(c.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
        to_char(c.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS updated_at,
        '{{ var('agency_id') }}' AS agency_id,
        a.id AS company_id,
        'prospect' AS role,
        c.title

    FROM 
        {{ var('source_database') }}."contact" c 
    LEFT JOIN {{ var('source_database') }}."account" a ON a.id = c."accountId"
    LEFT JOIN {{ var('source_database') }}."recordtype" r ON r.id = c.seven20__record_type_name__c
    WHERE 
        coalesce(r.name, c.seven20__record_type_name__c) = 'Client Contact'
)

SELECT b.*, ip.atlas_person_id, ic.atlas_company_id
FROM base b
LEFT JOIN internal_persons ip USING (person_id)
LEFT JOIN internal_companies ic USING (company_id)
