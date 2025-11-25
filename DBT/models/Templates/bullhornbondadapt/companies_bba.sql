{{ config(
    materialized='table',
    alias='companies_bba',
    tags=['bba']
) }}

WITH c AS (
  SELECT
      reference,
      client_id,
      "name",
      no_empl
  FROM {{ var('source_database') }}."prop_client_gen"
),
h AS (
  SELECT
      entity_id,
      createddate,
      updateddate,
      deleteddate
  FROM {{ var('source_database') }}."entity_table"
)

SELECT
    c.reference                                         AS id,
    {{ atlas_uuid('c.reference') }}                     AS atlas_id,
    TO_CHAR(h.createddate::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(h.updateddate::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    c."name"                                            AS name,
    NULL                                                AS summary,
    NULLIF(c.no_empl::text, '')                         AS size,
    'client'                                            AS relationship
FROM c
LEFT JOIN h
  ON h.entity_id = c.reference
WHERE h.deleteddate IS NULL
