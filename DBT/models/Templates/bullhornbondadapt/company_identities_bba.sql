{{ config(
    materialized='table',
    alias='company_identities_bba',
    tags=['bba']
) }}

WITH internal_companies AS (
  SELECT
      c.id       AS company_id,
      c.atlas_id AS atlas_company_id
  FROM {{ ref('companies_bba') }} c
),

h AS (
  SELECT
      entity_id,
      createddate,
      updateddate
  FROM {{ var('source_database') }}."entity_table"
),

base AS (
  SELECT
      pcg.reference                          AS company_id,
      {{linkedin_norm('pcg.web_add')}}       AS value,
      h.createddate                          AS created_dt,
      h.updateddate                          AS updated_dt
  FROM {{ var('source_database') }}."prop_client_gen" pcg
  LEFT JOIN h
    ON h.entity_id = pcg.reference
  WHERE pcg.web_add IS NOT NULL
    AND TRIM(pcg.web_add) <> ''
)

SELECT
    (b.company_id::text || '_website')                     AS id,
    {{ atlas_uuid('b.company_id::text || b.value') }}      AS atlas_id,
    b.company_id                                           AS company_id,
    ic.atlas_company_id                                    AS atlas_company_id,
    TO_CHAR(b.created_dt::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
    TO_CHAR(b.updated_dt::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
    LOWER(b.value)                                         AS value,
    'website'                                              AS type,
    TRUE                                                   AS "primary"
FROM base b
JOIN internal_companies ic USING (company_id)
