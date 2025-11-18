{{ config(
    materialized='table',
    alias='people_fr',
    tags=["seven20"]
) }}

WITH people_dup AS (
    SELECT 
        merge_person_id
    FROM {{ref('merged_map_fr')}}
)

SELECT 
	c.id as id,
    {{atlas_uuid('c.id')}} AS atlas_id, 
	c.firstname as first_name,
	c.lastname as last_name,
    to_char(c.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    to_char(c.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS updated_at,
	c.createdbyid as created_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    c.lastmodifiedbyid as updated_by_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
    '{{ var('agency_id') }}' AS agency_id,
    'active' AS responsiveness,
    'regular' AS candidate_status,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingstreet,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_street_address,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingcity,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_metro,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingstate,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingpostalcode,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_postal_code,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingcountry,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
    {{ build_location_locality
        ('c.mailingstreet', 'NULL', 'c.mailingcity', 'c.mailingstate', 'c.mailingpostalcode', 'c.mailingcountry')
    }} AS location_locality
FROM 
	{{ var('source_database') }}."contact" as c 
LEFT JOIN {{ var('source_database') }}.account a on a.id = c."accountid"
LEFT JOIN {{ref('1_users_720')}} u ON u.id = c.createdbyid
LEFT JOIN {{ref('1_users_720')}} u2 ON u2.id = c.lastmodifiedbyid
WHERE c.isdeleted = 0 AND c.id NOT IN (SELECT merge_person_id FROM people_dup)