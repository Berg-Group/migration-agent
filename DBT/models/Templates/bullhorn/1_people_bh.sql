{{ config(
    materialized = 'table',
    alias        = 'people_bh',
    tags         = ['bullhorn']
) }}

WITH base AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || UserID::text") }} AS atlas_id,
        c.UserID AS id,
        c.FirstName AS first_name,
        c.LastName AS last_name,
        TO_CHAR(c.DateLastModified::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        TO_CHAR(c.DateAdded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        'regular' AS candidate_status,
        'active' AS responsiveness,
        btrim(regexp_replace(coalesce(c.Address1,''), '[^a-zA-Z0-9 ]+', ' ')) AS address1,
        btrim(regexp_replace(coalesce(c.Address2,''), '[^a-zA-Z0-9 ]+', ' ')) AS address2,
        btrim(regexp_replace(coalesce(c.City,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_locality,
        btrim(regexp_replace(coalesce(c.State,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
        btrim(regexp_replace(coalesce({{ country_bh('c.CountryID') }}, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
        c.linkeduserid
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
    CASE 
        WHEN NULLIF(address1, '') IS NOT NULL AND NULLIF(address2, '') IS NOT NULL THEN address1 || ', ' || address2
        WHEN NULLIF(address1, '') IS NOT NULL THEN address1
        WHEN NULLIF(address2, '') IS NOT NULL THEN address2
        ELSE NULL
    END AS location_street_address,
    location_locality,
    location_region,
    location_country,
    NULL AS created_by_id,
    NULL AS updated_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    '{{ var("master_id") }}' AS updated_by_atlas_id,
    linkeduserid
FROM base
WHERE id NOT IN (SELECT contact_id FROM {{ ref('people_dupes_bh') }}) 
    AND id NOT IN (SELECT id FROM {{ ref('0_users_bh') }})