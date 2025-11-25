{{ config(
    materialized = 'table',
    alias = 'people_invenias',
    tags = ["invenias"]
) }}


with t as (
select person as person_id,
	  cl.fileas as location_locality,
	  row_number() over (partition by person)
from 
	{{ var('source_database') }}.relation_persontocategorylistentry pc
inner join {{ var('source_database') }}.categorylistentries cl on cl.itemid = pc.categorylistentryid
where cl.categorylistid = 'E5DB1242-A7C0-447A-9540-0CD5A899C96E'
and parentlistentryid notnull and cl.fileas <> 'Europe'),
 
base AS (
  SELECT
    p.itemid AS person_id,

    NULLIF(TRIM(p.homestreet),'') AS street_pick,
    NULLIF(TRIM(p.homepostalcode),'')  AS postal_pick,
    NULLIF(TRIM(p.homecity),'')  AS city_pick,
    NULLIF(TRIM(p.homestate),'')  AS region_pick,
    NULLIF(TRIM(COALESCE(p.homecountry, t.location_locality)),'') AS country_pick,

    p.creatorid,
    p.modifierid,
    p.firstname,
    p.familyname,
    p.datecreated,
    p.datemodified
  FROM {{ var('source_database') }}."people" p
LEFT JOIN t ON t.person_id = p.itemid and row_number = 1
  WHERE NOT (
    POSITION('@' IN COALESCE(p.firstname,'')) > 0
    OR POSITION('@' IN COALESCE(p.familyname,'')) > 0
    OR COALESCE(p.firstname,'') = COALESCE(p.email1address,'')
    OR COALESCE(p.familyname,'') = COALESCE(p.email1address,'')
))

SELECT
  base.person_id                                   AS id,
  {{ atlas_uuid('base.person_id') }}               AS atlas_id,
  base.firstname                                   AS first_name,
  base.familyname                                  AS last_name,
  NULLIF(
    TRIM(
      COALESCE(postal_pick,'') ||
      CASE WHEN COALESCE(postal_pick,'') <> '' AND COALESCE(street_pick,'') <> '' THEN ' ' ELSE '' END ||
      COALESCE(street_pick,'')
    )
  ,'')                                           AS location_street_address,

  NULLIF(
    TRIM(
      COALESCE(country_pick,'') ||
      CASE WHEN COALESCE(country_pick,'') <> '' AND COALESCE(region_pick,'') <> '' THEN ', ' ELSE '' END ||
      COALESCE(region_pick,'') ||
      CASE WHEN
        (COALESCE(country_pick,'') <> '' OR COALESCE(region_pick,'') <> '')
        AND COALESCE(city_pick,'') <> ''
      THEN ', ' ELSE '' END ||
      COALESCE(city_pick,'')
    )
  ,'')                                           AS location_locality,

  NULLIF(TRIM(city_pick),'')                   AS location_metro,
  NULLIF(TRIM(region_pick),'')                 AS location_region,
  NULLIF(TRIM(country_pick),'')                AS location_country,

  u.id                                           AS created_by_id,
  COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
  u2.id                                          AS updated_by_id,
  COALESCE(u2.atlas_id, '{{ var("master_id") }}')AS updated_by_atlas_id,

  TO_CHAR(base.datecreated::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
  TO_CHAR(base.datemodified::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,

  '{{ var("agency_id") }}'                       AS agency_id,
  'active'                                       AS responsiveness,
  'regular'                                      AS candidate_status

FROM base
LEFT JOIN {{ ref('users_invenias') }} u  ON u.id  = base.creatorid
LEFT JOIN {{ ref('users_invenias') }} u2 ON u2.id = base.modifierid
