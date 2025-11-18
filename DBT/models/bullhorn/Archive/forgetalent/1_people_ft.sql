{{ config(
    materialized = 'table',
    alias        = 'people_ft',
    tags         = ['bullhorn']
) }}

WITH base AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || UserID::text") }} AS atlas_id,
        c.UserID AS id,
        c.FirstName AS first_name,
        c.LastName AS last_name,
        TO_CHAR(c.DateLastModified::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        TO_CHAR(c.DateAdded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        'regular' AS candidate_status,
        'active' AS responsiveness,
        c.Address1 AS address1,
        c.City AS city,
        c.State AS state,
        c.CountryID AS country_id
    FROM {{ var("source_database") }}."bh_usercontact" c
    WHERE lower(c.FirstName) != 'default contact' AND c.isdeleted != 1
)
SELECT
    atlas_id,
    id,
    first_name,
    last_name,
    updated_at,
    created_at,
    candidate_status,
    responsiveness,
    btrim(regexp_replace(coalesce(address1,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_street_address,
    btrim(regexp_replace(coalesce(city,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_metro,
    btrim(regexp_replace(coalesce(state,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
    btrim(regexp_replace(coalesce({{ country_bh('country_id') }}, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
    {{ build_location_locality
        ('address1', 'NULL', 'city', 'state', 'NULL', 'location_country')
    }} AS location_locality,
    NULL AS created_by_id,
    NULL AS updated_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    '{{ var("master_id") }}' AS updated_by_atlas_id
FROM base
WHERE id NOT IN (SELECT contact_id FROM {{ ref('people_dupes_bh') }}) 
  AND id NOT IN (SELECT id FROM {{ ref('users_ft') }})