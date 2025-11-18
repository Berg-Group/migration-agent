{{ config(
    materialized = 'table',
    alias = 'people_bba',
    tags = ["bba"]
) }}

WITH p AS (
  SELECT
    reference,
    first_name,
    last_name
  FROM {{ var('source_database') }}."prop_person_gen" p
),
h AS (
  SELECT
    entity_id,
    created_by,
    createddate,
    updated_by,
    updateddate,
    deleteddate
  FROM {{ var('source_database') }}."entity_table"
),
addr_choices AS (
  SELECT
    a.reference,
    a.street1,
    a.street2,
    a.town,
    a.county,
    a.post_code,
    a.country,
    ROW_NUMBER() OVER (
      PARTITION BY a.reference
      ORDER BY CASE WHEN NULLIF(TRIM(a.street1),'') IS NULL THEN 1 ELSE 0 END,
               a.bisuniqueid
    ) AS rn
  FROM {{ var('source_database') }}."prop_address" a
),
a AS (
  SELECT
    reference,
    street1,
    street2,
    town,
    county,
    post_code,
    country
  FROM addr_choices
  WHERE rn = 1
),
location_parts AS (
  SELECT 
    reference,
    CASE 
      WHEN NULLIF(TRIM(street1), '') IS NOT NULL 
        AND UPPER(NULLIF(TRIM(street1), '')) NOT IN ('0','00','000','0000','00000','-','.','NA','N/A','NULL','NONE')
        AND NULLIF(TRIM(street1), '') NOT LIKE '%linkedin.com%'
        AND NULLIF(TRIM(street1), '') NOT LIKE '%http%'
        AND NULLIF(TRIM(street1), '') NOT LIKE '%www.%'
      THEN NULLIF(TRIM(street1), '')
      ELSE NULL 
    END AS street1_clean,
    CASE 
      WHEN NULLIF(TRIM(street2), '') IS NOT NULL 
        AND UPPER(NULLIF(TRIM(street2), '')) NOT IN ('0','00','000','0000','00000','-','.','NA','N/A','NULL','NONE')
        AND NULLIF(TRIM(street2), '') NOT LIKE '%linkedin.com%'
        AND NULLIF(TRIM(street2), '') NOT LIKE '%http%'
        AND NULLIF(TRIM(street2), '') NOT LIKE '%www.%'
      THEN NULLIF(TRIM(street2), '')
      ELSE NULL 
    END AS street2_clean,
    CASE 
      WHEN NULLIF(TRIM(town), '') IS NOT NULL 
        AND UPPER(NULLIF(TRIM(town), '')) NOT IN ('0','00','000','0000','00000','-','.','NA','N/A','NULL','NONE')
        AND NULLIF(TRIM(town), '') NOT LIKE '%linkedin.com%'
        AND NULLIF(TRIM(town), '') NOT LIKE '%http%'
        AND NULLIF(TRIM(town), '') NOT LIKE '%www.%'
      THEN NULLIF(TRIM(town), '')
      ELSE NULL 
    END AS town_clean,
    CASE 
      WHEN NULLIF(TRIM(county), '') IS NOT NULL 
        AND UPPER(NULLIF(TRIM(county), '')) NOT IN ('0','00','000','0000','00000','-','.','NA','N/A','NULL','NONE')
        AND NULLIF(TRIM(county), '') NOT LIKE '%linkedin.com%'
        AND NULLIF(TRIM(county), '') NOT LIKE '%http%'
        AND NULLIF(TRIM(county), '') NOT LIKE '%www.%'
      THEN NULLIF(TRIM(county), '')
      ELSE NULL 
    END AS county_clean,
    CASE 
      WHEN NULLIF(TRIM(post_code), '') IS NOT NULL 
        AND UPPER(NULLIF(TRIM(post_code), '')) NOT IN ('0','00','000','0000','00000','-','.','NA','N/A','NULL','NONE')
        AND NULLIF(TRIM(post_code), '') NOT LIKE '%linkedin.com%'
        AND NULLIF(TRIM(post_code), '') NOT LIKE '%http%'
        AND NULLIF(TRIM(post_code), '') NOT LIKE '%www.%'
      THEN NULLIF(TRIM(post_code), '')
      ELSE NULL 
    END AS post_code_clean,
    CASE 
      WHEN NULLIF(TRIM(country), '') IS NOT NULL 
        AND UPPER(NULLIF(TRIM(country), '')) NOT IN ('0','00','000','0000','00000','-','.','NA','N/A','NULL','NONE')
        AND NULLIF(TRIM(country), '') NOT LIKE '%linkedin.com%'
        AND NULLIF(TRIM(country), '') NOT LIKE '%http%'
        AND NULLIF(TRIM(country), '') NOT LIKE '%www.%'
        AND NULLIF(TRIM(country), '') ~ '^[0-9]+$' = FALSE
      THEN NULLIF(TRIM(country), '')
      ELSE NULL 
    END AS country_clean
  FROM a
),
loc AS (
  SELECT
    reference,
    CASE 
      WHEN street1_clean IS NOT NULL AND town_clean IS NOT NULL AND post_code_clean IS NOT NULL 
      THEN street1_clean || ', ' || town_clean || ', ' || post_code_clean
      WHEN street1_clean IS NOT NULL AND town_clean IS NOT NULL 
      THEN street1_clean || ', ' || town_clean
      WHEN town_clean IS NOT NULL AND post_code_clean IS NOT NULL 
      THEN town_clean || ', ' || post_code_clean
      WHEN town_clean IS NOT NULL 
      THEN town_clean
      WHEN post_code_clean IS NOT NULL 
      THEN post_code_clean
      WHEN county_clean IS NOT NULL 
      THEN county_clean
      WHEN country_clean IS NOT NULL 
      THEN country_clean
      ELSE NULL
    END AS location_locality,
    street1_clean AS location_street_address,
    town_clean AS location_name,
    county_clean AS location_metro,
    country_clean AS location_country
  FROM location_parts
)

SELECT
  p.reference AS id,
  {{ atlas_uuid('p.reference') }} AS atlas_id,
  p.first_name,
  p.last_name,
  h.created_by AS created_by_id,
  '{{ var("master_id") }}' AS created_by_atlas_id,
  h.updated_by AS updated_by_id,
  '{{ var("master_id") }}' AS updated_by_atlas_id,
  TO_CHAR(h.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
  TO_CHAR(h.updateddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
  '{{ var("agency_id") }}' AS agency_id,
  'active'  AS responsiveness,
  'regular' AS candidate_status,
  CASE 
    WHEN loc.location_locality IS NOT NULL THEN loc.location_locality
    WHEN loc.location_street_address IS NOT NULL AND loc.location_name IS NOT NULL 
    THEN loc.location_street_address || ', ' || loc.location_name
    WHEN loc.location_street_address IS NOT NULL 
    THEN loc.location_street_address
    WHEN loc.location_name IS NOT NULL 
    THEN loc.location_name
    WHEN loc.location_metro IS NOT NULL 
    THEN loc.location_metro
    WHEN loc.location_country IS NOT NULL 
    THEN loc.location_country
    ELSE NULL
  END AS location_locality,
  loc.location_street_address,
  loc.location_name,
  loc.location_metro,
  loc.location_country
FROM p
LEFT JOIN h   ON h.entity_id = p.reference
LEFT JOIN loc ON loc.reference = p.reference
WHERE h.deleteddate IS NULL