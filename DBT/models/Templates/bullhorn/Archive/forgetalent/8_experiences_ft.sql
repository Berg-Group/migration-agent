{{ config(
    materialized = 'table',
    alias        = 'experiences_ft',
    tags         = ['bullhorn']
) }}

WITH source_experiences AS (
    SELECT
        exp."UserWorkHistoryID"      AS id,
        LEFT(exp."startDate", 10)    AS started_at,
        LEFT(exp."endDate",   10)    AS finished_at,
        exp."CompanyName"            AS company_name,
        exp."title"                  AS title,
        exp."comments"               AS raw_description,
        exp."userID"                 AS person_id
    FROM {{ var('source_database') }}."BH_UserWorkHistory" exp
    WHERE 
        (exp."CompanyName" IS NOT NULL AND TRIM(exp."CompanyName") <> '')
        AND LEFT(exp."startDate", 10) IS NOT NULL
),
company_ids AS (
    SELECT 
        name AS company_name,
        id,
        atlas_id AS atlas_company_id
    FROM {{ ref('3_companies_bh') }}
),
regular_experiences AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || se.id") }} AS atlas_id,
        se.id,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        CAST('migration' AS varchar) AS source,
        CAST('{{ var("agency_id") }}'AS varchar) AS agency_id,
        se.started_at,
        se.finished_at,
        se.company_name,
        se.title,
        REGEXP_REPLACE(
            CAST(COALESCE(se.raw_description,'') AS varchar),
            '<[^>]+>',
            '',
            1
        )                                     AS description,
        se.person_id,
        p.atlas_id AS atlas_person_id,
        ci.id AS company_id,
        ci.atlas_company_id
    FROM source_experiences se
    INNER JOIN {{ ref('1_people_ft') }} p ON se.person_id = p.id
    LEFT JOIN company_ids ci ON LOWER(TRIM(se.company_name)) = LOWER(TRIM(ci.company_name))
    WHERE (se.started_at IS NOT NULL AND se.started_at != '')
        AND (se.title IS NOT NULL AND se.title != '')
),
dupe_people_experiences AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || se.id") }} AS atlas_id,
        se.id,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        CAST('migration' AS varchar) AS source,
        CAST('{{ var("agency_id") }}'AS varchar) AS agency_id,
        se.started_at,
        se.finished_at,
        se.company_name,
        se.title,
        REGEXP_REPLACE(
            CAST(COALESCE(se.raw_description,'') AS varchar),
            '<[^>]+>',
            '',
            1
        )                                     AS description,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        ci.id AS company_id,
        ci.atlas_company_id
    FROM source_experiences se
    INNER JOIN {{ ref('people_dupes_bh') }} pd ON pd.contact_id = se.person_id
    INNER JOIN {{ ref('1_people_ft') }} p ON p.id = pd.candidate_id
    LEFT JOIN company_ids ci ON LOWER(TRIM(se.company_name)) = LOWER(TRIM(ci.company_name))
    WHERE (se.started_at IS NOT NULL AND se.started_at != '')
        AND (se.title IS NOT NULL AND se.title != '')
)
SELECT 
    id,
    atlas_id,
    created_at,
    updated_at,
    source,
    agency_id,
    started_at,
    finished_at,
    company_name,
    title,
    description,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM regular_experiences
        UNION ALL
        SELECT * FROM dupe_people_experiences
    ) combined
) final
WHERE rn = 1