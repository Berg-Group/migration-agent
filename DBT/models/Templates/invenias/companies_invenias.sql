{{ config(
    materialized = 'table',
    alias = 'companies_invenias',
    tags = ["invenias"]
) }}

WITH project_check AS (
  SELECT DISTINCT
    rca."companyid"
  FROM {{ var('source_database') }}."relation_companytoassignment" rca
),

company_location_settings AS (
  SELECT
    cls."companylocationid",
    MAX(CASE WHEN cls."settingname" = 'CityFieldLabel'     THEN cls."settingvalue" END)     AS city_label,
    MAX(CASE WHEN cls."settingname" = 'PostcodeFieldLabel' THEN cls."settingvalue" END)     AS postcode_label,
    MAX(CASE WHEN cls."settingname" = 'CountyFieldLabel'   THEN cls."settingvalue" END)     AS county_label
  FROM {{ var('source_database') }}."companylocationsettings" cls
  GROUP BY cls."companylocationid"
),

company_location_lookup AS (
  SELECT
    rcl."companyid"                                          AS company_id,
    l."itemid"                                               AS location_id,
    COALESCE(l."businesspostalcode" || ' ' || l."businessstreet", '') AS loc_street_address,
    COALESCE(
      NULLIF(l."businesscountry",'') ||
      CASE WHEN COALESCE(l."businesscountry",'') <> '' THEN ', ' ELSE '' END ||
      NULLIF(l."businessstate",'') ||
      CASE WHEN COALESCE(l."businessstate",'') <> '' THEN ', ' ELSE '' END ||
      NULLIF(l."businesscity",'') ||
      CASE WHEN COALESCE(l."businesscity",'') <> '' THEN ', ' ELSE '' END ||
      NULLIF(l."businesspostalcode",'') ||
      CASE WHEN COALESCE(l."businesspostalcode",'') <> '' THEN ', ' ELSE '' END ||
      NULLIF(l."businessstreet",'')
    ,'')                                                     AS loc_locality,
    COALESCE(l."businesscity",    cls.city_label)            AS loc_metro,
    COALESCE(l."businessstate",   cls.county_label)          AS loc_region,
    COALESCE(l."businesscountry", '')                        AS loc_country,
    COALESCE(l."businesspostalcode", cls.postcode_label)     AS loc_postalcode,
    ROW_NUMBER() OVER (
      PARTITION BY rcl."companyid"
      ORDER BY COALESCE(rcl."datemodified", rcl."datecreated") DESC,
               COALESCE(l."datemodified",  l."datecreated")  DESC
    )                                                        AS rn
  FROM {{ var('source_database') }}."relation_companytolocation" rcl
  JOIN {{ var('source_database') }}."locations" l
    ON l."itemid" = rcl."locationid"
  LEFT JOIN company_location_settings cls
    ON cls."companylocationid" = l."itemid"
),

loc AS (
  SELECT
    company_id,
    loc_street_address,
    loc_locality,
    loc_metro,
    loc_region,
    loc_country,
    loc_postalcode
  FROM company_location_lookup
  WHERE rn = 1
)

SELECT
  c."itemid"                                                                 AS id,
  {{ atlas_uuid('c.itemid') }}                                               AS atlas_id,
  c."fileas"                                                                 AS name,

  COALESCE(loc.loc_street_address, '')                                       AS location_street_address,
  COALESCE(loc.loc_locality,       '')                                       AS location_locality,
  COALESCE(loc.loc_metro,          '')                                       AS location_metro,
  COALESCE(loc.loc_region,         '')                                       AS location_region,
  COALESCE(loc.loc_country,        '')                                       AS location_country,
  COALESCE(loc.loc_postalcode,     '')                                       AS location_postalcode,

  u."id"                                                                     AS created_by_id,
  COALESCE(u."atlas_id", '{{ var("master_id") }}')                           AS created_by_atlas_id,
  TO_CHAR(c."datecreated"::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS')         AS created_at,
  TO_CHAR(c."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')         AS updated_at,
  u2."id"                                                                    AS updated_by_id,
  COALESCE(u2."atlas_id", '{{ var("master_id") }}')                          AS updated_by_atlas_id,

  '{{ var("agency_id") }}'                                                   AS agency_id,

  CASE
    WHEN LOWER(lcs."name") = 'current'            THEN 'client'
    WHEN LOWER(lcs."name") IN ('prospect','cold') THEN 'target'
    ELSE 'none'
  END                                                                         AS relationship,

  CASE WHEN c."offmarketstatus" = 1 THEN 'hard' ELSE NULL END                 AS restriction_type,
  CASE WHEN c."offmarketstatus" = 1 THEN TO_CHAR(c."offmarketstartdate"::timestamp(0),'YYYY-MM-DD"T"00:00:00') END
                                                                              AS restriction_created_at,
  NULL                                                                        AS restriction_expiry_date,
  CASE WHEN c."offmarketstatus" = 1 THEN COALESCE(c."offmarketownerid",'{{ var("master_id") }}') END
                                                                              AS restriction_created_by_id,
  CASE WHEN c."offmarketstatus" = 1 THEN c."offmarketreason" END              AS restriction_note

FROM {{ var('source_database') }}."companies" c
LEFT JOIN {{ ref('users_invenias') }} u
  ON u."id" = c."creatorid"
LEFT JOIN {{ ref('users_invenias') }} u2
  ON u2."id" = c."modifierid"
LEFT JOIN {{ var('source_database') }}."lookuplistentries" lcs
  ON lcs."itemid" = c."clientstatus"
LEFT JOIN project_check pc
  ON pc."companyid" = c."itemid"
LEFT JOIN loc
  ON loc.company_id = c."itemid"
