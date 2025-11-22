{{ config(
    materialized='table',
    alias='experiences_ff',
    tags=["filefinder"]
) }}

WITH source_experiences AS (
    SELECT 
        cp.idcompany_person AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || cp.idcompany_person::text") }} AS atlas_id,
        p.idPerson AS person_id,
        pf.atlas_id AS atlas_person_id,
        cp.JobTitle AS title,
        cp.companynameonly AS company_name,
        cp.idCompany AS company_id,
        cf.atlas_id AS atlas_company_id,
        CASE 
            WHEN cp.employmentfrom IS NOT NULL THEN TO_CHAR(cp.employmentfrom::timestamp(0), 'YYYY-MM-DD')
            ELSE '2000-01-01'
        END AS started_at,
        CASE 
            WHEN cp.employmentto IS NOT NULL THEN TO_CHAR(cp.employmentto::timestamp(0), 'YYYY-MM-DD')
            ELSE NULL
        END AS finished_at,
        regexp_replace(
                cp.note,
                '<[^>]+>',
                ' ',
                1,
                'i'
        ) AS description,
        'migration' AS source,
        TO_CHAR(cp.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(cp.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Company_Person cp 
    INNER JOIN {{ var('source_database') }}.Person p ON p.idPerson = cp.idPerson 
    INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = p.idPerson
    LEFT JOIN {{ this.schema }}.companies_ff cf ON cf.id = cp.idCompany
    WHERE (cp.JobTitle IS NOT NULL AND cp.JobTitle <> '')
)
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    title,
    company_name,
    started_at,
    finished_at,
    CASE
        WHEN TRIM(description) = '' THEN NULL
        ELSE description
    END AS description,
    source,
    created_at,
    updated_at,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC
        ) AS rn
    FROM source_experiences
) deduped
WHERE rn = 1