{{ config(
    materialized='table',
    alias='correction_peoplelocation'
) }}

WITH people AS (
    SELECT
        p.id       AS person_id,
        p.atlas_id AS atlas_person_id
    FROM {{ ref('1_people_ja') }} p
),

-- Existing "resume_parsing" data
resume_parsing AS (
    SELECT
        c.contactid,
        c.addresspostcode,
        c.addressline1,
        c.addressline2,
        c.addressstate,
        c.addresscountry
    FROM {{ var('source_database') }}."candidateresumeparsing" c
),

-- New CTE: address data from the "candidate" table
candidate AS (
    SELECT
        can.contactid,
        can.addresspostcode,
        can.addressline1,
        can.addressline2,
        can.addresssuburb,         -- same as 'addressstate'
        can.addresscountrycode
    FROM {{ var('source_database') }}."candidate" can
)

SELECT
    p.person_id                       AS person_id,
    p.atlas_person_id                 AS atlas_person_id,

    -- Prefer candidate table columns when present
    COALESCE(can.addresspostcode,  r.addresspostcode)  AS location_postal_code,
    COALESCE(can.addressline1,     r.addressline1)     AS location_street_address,
    COALESCE(can.addressline2,     r.addressline2)     AS location_address_line_2,
    COALESCE(can.addresssuburb,    r.addressstate)     AS location_metro,
    COALESCE(can.addresscountrycode, r.addresscountry) AS location_country,

    -- Build location_locality string using whichever table has data (preferring candidate)
    REGEXP_REPLACE(
      REGEXP_REPLACE(
          COALESCE(can.addressline1,  r.addressline1,  '') || ',' ||
          COALESCE(can.addressline2,  r.addressline2,  '') || ',' ||
          COALESCE(can.addresssuburb, r.addressstate,   '') || ',' ||
          COALESCE(can.addresspostcode, r.addresspostcode, '') || ',' ||
          COALESCE(can.addresscountrycode, r.addresscountry, ''),
        '(^,*)|(,*$)',  -- remove any leading or trailing commas
        ''
      ),
      ',+',          -- replace repeated commas
      ', '           -- with a single comma+space
    ) AS location_locality

FROM people p
    LEFT JOIN resume_parsing r 
        ON p.person_id = r.contactid
    LEFT JOIN candidate can
        ON p.person_id = can.contactid
