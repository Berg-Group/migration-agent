-- File: models/experiences_ja.sql

{{ config(
    materialized='table',
    alias='experiences_ja'
) }}

WITH source_data AS (
    SELECT
        eh.employmentid AS id,
        eh.startdate,
        eh.enddate,
        eh.employer      AS company_name,
        {{ html_to_markdown('eh.description') }} AS description,
        eh.position      AS title,
        eh.contactid     AS person_id,
        eh.employmentid::TEXT || '{{ var("agency_id") }}' AS uuid_input
    FROM {{ var('source_database') }}."candidateemploymenthistory" eh
),

company_lookup AS (
    SELECT
        name      AS company_name,
        id        AS company_id,
        atlas_id  AS atlas_company_id,
        ROW_NUMBER() OVER (PARTITION BY name)
    FROM {{ ref('3_companies_ja') }}

),

person_lookup AS (
    SELECT
        id        AS person_id,
        atlas_id  AS atlas_person_id
    FROM {{ ref('1_people_ja') }}
)

SELECT
    sd.id,
    {{ atlas_uuid('sd.uuid_input') }} AS atlas_id,
    '{{ var("master_id") }}'     AS created_by_id,
    '{{ var("agency_id") }}'     AS agency_id,
    sd.person_id,
    pl.atlas_person_id,
    TRIM(sd.company_name) AS company_name,
    cl.company_id,
    cl.atlas_company_id,
    LEFT(sd.startdate::TEXT, 10) AS started_at,
    LEFT(sd.enddate::TEXT, 10)   AS finished_at,
    sd.description,
    sd.title,
    'migration' AS source
FROM source_data sd
LEFT JOIN company_lookup cl ON cl.company_name = sd.company_name AND row_number = 1
LEFT JOIN person_lookup pl USING (person_id)
WHERE sd.startdate IS NOT NULL
  AND TRIM(sd.company_name) != ''
  AND sd.title IS NOT NULL
  AND TRIM(sd.title) <> ''
