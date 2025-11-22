{{ config(
    materialized = 'table',
    alias = 'company_identities_invenias',
    tags = ["invenias"]
) }}

WITH internal_companies AS (
    SELECT 
        id         AS company_id,
        atlas_id   AS atlas_company_id
    FROM {{ ref('companies_invenias') }}
),

cleaned_websites AS (
    SELECT
        c."itemid" AS company_id,
        TRIM(BOTH '/' FROM REPLACE(REPLACE(REPLACE(c."webpage",'https://',''),'http://',''),'www.','')) AS value,
        {{ atlas_uuid('c.webpage') }} AS atlas_id,
        TO_CHAR(c."datecreated"::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        'website'   AS type,
        '{{ var("agency_id") }}' AS agency_id,
        TRUE        AS is_primary
    FROM {{ var('source_database') }}."companies" c
    WHERE c."webpage" IS NOT NULL AND c."webpage" <> '' AND c."webpage" <> ' '
    AND POSITION('linkedin.com' IN LOWER(c."webpage")) = 0
),

cleaned_linkedin AS (
    SELECT
        c."itemid" AS company_id,
        TRIM(BOTH '/' FROM REPLACE(REPLACE(REPLACE(c."linkedin",'https://',''),'http://',''),'www.','')) AS value,
        {{ atlas_uuid('c.linkedin') }} AS atlas_id,
        TO_CHAR(c."datecreated"::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        'linkedin' AS type,
        '{{ var("agency_id") }}' AS agency_id,
        FALSE       AS is_primary
    FROM {{ var('source_database') }}."companies" c
    WHERE c."linkedin" IS NOT NULL AND POSITION('linkedin.com' IN c."linkedin") > 0
),

base AS (
    SELECT * FROM cleaned_websites
    UNION ALL
    SELECT * FROM cleaned_linkedin
),

dedup AS (
    SELECT
        b.company_id,
        b.value,
        b.atlas_id,
        b.created_at,
        b.type,
        b.agency_id,
        b.is_primary,
        ROW_NUMBER() OVER (
            PARTITION BY b.value, b.type
            ORDER BY b.created_at ASC
        ) AS rn
    FROM base b
)

SELECT
    d.company_id,
    d.value,
    d.atlas_id,
    d.created_at,
    d.type,
    d.agency_id,
    d.is_primary,
    ic.atlas_company_id
FROM dedup d
LEFT JOIN internal_companies ic USING (company_id)
WHERE d.rn = 1