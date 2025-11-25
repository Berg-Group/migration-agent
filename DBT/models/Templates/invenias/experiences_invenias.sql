{{ config(
    materialized = 'table',
    alias = 'experiences_invenias',
    tags=["invenias"]
) }}
with internal_persons AS (
SELECT
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM
    {{ref('people_invenias')}}
),

internal_companies AS (
SELECT 
    DISTINCT id AS company_id,
    atlas_id AS atlas_company_id,
    name
FROM 
    {{ref('companies_invenias')}}
),

t AS (SELECT 
    itemid AS id,
    {{atlas_uuid('itemid')}} AS atlas_id,
    startdate::DATE AS started_at,
    enddate::DATE AS finished_at,
    pp.personid AS person_id,
    internal_persons.atlas_person_id,
    CASE WHEN jobtitle LIKE '%@%'
         THEN TRIM(SPLIT_PART(jobtitle, '@', 1))
         ELSE TRIM(jobtitle)
    END AS title,
    COALESCE(CASE WHEN jobtitle LIKE '%@%'
         THEN TRIM(SPLIT_PART(jobtitle, '@', 2))
         ELSE NULL END, internal_companies.name) AS company_name,
    description,
    cp.companyid AS company_id,
    internal_companies.atlas_company_id,
    '{{ var('agency_id') }}' AS agency_id,
    'migration' AS source
FROM 
    {{ var('source_database') }}."positions" p 
LEFT JOIN 
    {{ var('source_database') }}."relation_persontoposition" pp ON pp.positionid = p.itemid
LEFT JOIN 
    {{ var('source_database') }}."relation_companytoposition" cp ON cp.positionid = pp.positionid
LEFT JOIN 
    internal_companies ON internal_companies.company_id = cp.companyid
INNER JOIN 
    internal_persons ON internal_persons.person_id = pp.personid)

SELECT * FROM t
WHERE 
    started_at IS NOT NULL 
    AND NULLIF(TRIM(title), '') IS NOT NULL
    AND NULLIF(TRIM(company_name), '') IS NOT NULL 