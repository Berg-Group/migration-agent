{{ config(materialized='table', alias='people_rcrm') }}

{% set db = var('source_database') %}

WITH candidate_people AS (

    SELECT
        cd.slug                               AS id, 
        cd.slug                               AS candidate_slug,
        MIN(ct.slug)                          AS contact_slug, 

        cd.first_name,
        cd.last_name,

        {{ atlas_uuid('cd.slug') }}           AS atlas_id,

        TO_CHAR(
            DATE_TRUNC('day',
              TIMESTAMP 'epoch' + (cd.created_on::bigint)*INTERVAL '1 second'),
            'YYYY-MM-DD"T00:00:00"')          AS created_at,
        TO_CHAR(
            DATE_TRUNC('day',
              TIMESTAMP 'epoch' + (cd.updated_on::bigint)*INTERVAL '1 second'),
            'YYYY-MM-DD"T00:00:00"')          AS updated_at,

        cd.created_by                         AS created_by_id,
        {{ atlas_uuid('cd.created_by') }}     AS created_by_atlas_id,
        cd.created_by                         AS updated_by_id,
        {{ atlas_uuid('cd.created_by') }}     AS updated_by_atlas_id,

        'active'  AS responsiveness,
        'regular' AS candidate_status,

        cd.fulladdress AS location_street_address,
        cd.fulladdress AS location_locality,
        cd.city        AS location_metro,
        cd.state       AS location_region,
        cd.country     AS location_country

    FROM {{ db }}.candidate_data cd
    LEFT JOIN {{ db }}.contact_data  ct
           ON  LOWER(TRIM(cd.email))            = LOWER(TRIM(ct.email))
           OR  TRIM(cd.contact_number)          = TRIM(ct.contact_number)
           OR  LOWER(TRIM(cd.profile_linkedin)) = LOWER(TRIM(ct.profile_linkedin))

    GROUP BY
        cd.slug, cd.first_name, cd.last_name,
        cd.created_on, cd.updated_on, cd.created_by,
        cd.fulladdress, cd.city, cd.state, cd.country
),


contacts_only AS (

    SELECT
        'cc' || ct.slug                       AS id,  
        NULL                                   AS candidate_slug,
        ct.slug                                AS contact_slug,

        ct.first_name,
        ct.last_name,

        {{ atlas_uuid("'cc' || ct.slug") }}   AS atlas_id,

        TO_CHAR(
            DATE_TRUNC('day',
              TIMESTAMP 'epoch' + (ct.created_on::bigint)*INTERVAL '1 second'),
            'YYYY-MM-DD"T00:00:00"')          AS created_at,
        TO_CHAR(
            DATE_TRUNC('day',
              TIMESTAMP 'epoch' + (ct.updated_on::bigint)*INTERVAL '1 second'),
            'YYYY-MM-DD"T00:00:00"')          AS updated_at,

        ct.created_by                         AS created_by_id,
        {{ atlas_uuid('ct.created_by') }}     AS created_by_atlas_id,
        ct.created_by                         AS updated_by_id,
        {{ atlas_uuid('ct.created_by') }}     AS updated_by_atlas_id,

        'active'  AS responsiveness,
        'regular' AS candidate_status,

        ct.full_address                       AS location_street_address,
        {{ concat('ct.full_address,ct.city,ct.state') }} AS location_locality,
        ct.city                               AS location_metro,
        ct.state                               AS location_region,
        ''                                    AS location_country 

    FROM {{ db }}.contact_data ct
    LEFT JOIN {{ db }}.candidate_data cd
           ON  LOWER(TRIM(cd.email))            = LOWER(TRIM(ct.email))
           OR  TRIM(cd.contact_number)          = TRIM(ct.contact_number)
           OR  LOWER(TRIM(cd.profile_linkedin)) = LOWER(TRIM(ct.profile_linkedin))

    WHERE cd.slug IS NULL 
),

base_people AS (
    SELECT * FROM candidate_people
    UNION ALL
    SELECT * FROM contacts_only
)

SELECT
    id,
    candidate_slug,
    contact_slug,
    first_name,
    last_name,
    atlas_id,
    created_at,
    updated_at,
    created_by_id,
    created_by_atlas_id,
    updated_by_id,
    updated_by_atlas_id,
    responsiveness,
    candidate_status,
    location_street_address,
    CASE
        WHEN COALESCE(location_locality, '') = ''
        THEN TRIM(BOTH ' ' FROM (location_metro || ' ' || location_country))
        ELSE location_locality
    END               AS location_locality,

    location_metro,
    location_region,
    location_country

FROM base_people