{{ config(
    materialized='table',
    alias='experiences_720',
    tags=["seven20"]
) }}

with internal_persons AS (
SELECT
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM
    "{{ this.schema }}"."people"
),

internal_companies AS (
SELECT 
    DISTINCT id AS company_id,
    atlas_id AS atlas_company_id,
    name
FROM 
     "{{ this.schema }}"."companies"   
)

SELECT 
    h.id,
    lower(
            substring(md5(h.id::text || '-' || coalesce(ip.person_id::text, 'NULL')), 1, 8) || '-' ||
            substring(md5(h.id::text || '-' || coalesce(ip.person_id::text, 'NULL')), 9, 4) || '-' ||
            substring(md5(h.id::text || '-' || coalesce(ip.person_id::text, 'NULL')), 13, 4) || '-' ||
            substring(md5(h.id::text || '-' || coalesce(ip.person_id::text, 'NULL')), 17, 4) || '-' ||
            substring(md5(h.id::text || '-' || coalesce(ip.person_id::text, 'NULL')), 21, 12)
        ) AS atlas_id,
    to_char(h.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    h.seven20__start_date__c::DATE AS started_at,
    h.seven20__end_date__c::DATE AS finished_at,
    ip.person_id,
    ip.atlas_person_id,
    ic.company_id,
    ic.atlas_company_id,
    h.seven20__account__c AS company_name,
    h.seven20__description__c AS description,
    h.seven20__job_title__c AS title,
    u.atlas_id AS created_by_atlas_id,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    {{ var('source_database') }}."seven20__employment_history__c" h 
LEFT JOIN 
    internal_persons ip ON ip.person_id = h.seven20__candidate__c
LEFT JOIN 
    internal_companies ic ON ic.name = h."seven20__account__c"
LEFT JOIN
    "{{ this.schema }}"."users" u ON u.id = h.createdbyid
WHERE
    h.seven20__account__c NOTNULL OR ic.atlas_company_id NOTNULL 