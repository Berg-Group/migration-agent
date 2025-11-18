{{ config(
    materialized = 'table',
    alias = 'company_contacts_bba',
    tags = ['bba']
) }}

WITH internal_persons AS (
  SELECT
      id::text     AS person_id,
      atlas_id     AS atlas_person_id
  FROM {{ ref('people_bba') }}
),
internal_companies AS (
  SELECT
      id::text     AS company_id,
      atlas_id     AS atlas_company_id
  FROM {{ ref('companies_bba') }}
),
h AS (
  SELECT
      entity_id,
      createddate,
      updateddate,
      deleteddate
  FROM {{ var('source_database') }}."entity_table"
),
base AS (
  SELECT
      pcg.bisuniqueid::text                                     AS id,
      {{ atlas_uuid('pcg.bisuniqueid::text || pcg.cur_empl::text') }} AS atlas_id,
      TO_CHAR(h.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
      TO_CHAR(h.updateddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
      '{{ var("agency_id") }}'                                   AS agency_id,
      pcg.reference::text                                        AS person_id,
      pcg.cur_empl::text                                         AS company_id,
      'client'                                                   AS relationship,
        job_title                                                AS title
  FROM {{ var('source_database') }}."prop_cont_gen" pcg
  LEFT JOIN h
    ON h.entity_id = pcg.reference
  WHERE h.deleteddate IS NULL
)

SELECT
    b.id,
    b.atlas_id,
    b.person_id,
    ip.atlas_person_id,
    b.created_at,
    b.updated_at,
    b.agency_id,
    b.company_id,
    ic.atlas_company_id,
    b.relationship,
    b.title
FROM base b
JOIN internal_persons ip
  ON ip.person_id = b.person_id
INNER JOIN internal_companies ic
  ON ic.company_id = b.company_id
WHERE NULLIF(TRIM(b.title), '') IS NOT NULL
