{{ config(
  materialized = 'table',
  alias = 'people_loxo',
  tags = ["loxo"]
) }}

SELECT
  p.id,
  {{ atlas_uuid("'" ~ var('clientName') ~ "' || p.id::text") }} AS atlas_id,
  SPLIT_PART(p.name, ' ', 1) AS first_name,
  CASE 
    WHEN POSITION(' ' IN p.name) > 0 THEN SUBSTRING(p.name FROM POSITION(' ' IN p.name) + 1)
    ELSE NULL
  END AS last_name,
  p."desc" AS overview,
  TO_CHAR(TRY_CAST(p.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
  TO_CHAR(TRY_CAST(p.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
  'active' AS responsiveness,
  'regular' AS candidate_status,
  BTRIM(REGEXP_REPLACE(COALESCE(p.city,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_locality,
  BTRIM(REGEXP_REPLACE(COALESCE(p.state,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
  BTRIM(REGEXP_REPLACE(COALESCE(p.zip,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_postal_code,
  BTRIM(REGEXP_REPLACE(COALESCE(p.country,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
  NULL AS created_by_id,
  NULL AS updated_by_id,
  '{{ var("master_id") }}' AS created_by_atlas_id,
  '{{ var("master_id") }}' AS updated_by_atlas_id,
  '{{ var("agency_id") }}' AS agency_id
FROM {{ var('source_database') }}.people p