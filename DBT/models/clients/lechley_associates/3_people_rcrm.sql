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
        {{ concat('cd.fulladdress,cd.locality,cd.city,cd.state,cd.country') }} AS location_locality,
        NULLIF(
            TRIM(
                COALESCE(
                    cd.city,
                    cd.locality,
                    MIN(ct.city),
                    MIN(ct.locality)
                )
            ),
            ''
        )                                                       AS location_metro,
        NULLIF(TRIM(cd.state), '')                              AS location_region,
        NULLIF(TRIM(cd.country), '')                            AS location_country,
        NULLIF(TRIM(cd.country_of_passport), '')                AS location_fallback_label

    FROM {{ db }}.candidate_data cd
    LEFT JOIN {{ db }}.contact_data  ct
           ON  LOWER(TRIM(cd.email))            = LOWER(TRIM(ct.email))
           OR  TRIM(cd.contact_number)          = TRIM(ct.contact_number)
           OR  LOWER(TRIM(cd.profile_linkedin)) = LOWER(TRIM(ct.profile_linkedin))

    GROUP BY
        cd.slug, cd.first_name, cd.last_name,
        cd.created_on, cd.updated_on, cd.created_by,
        cd.fulladdress, cd.locality, cd.city, cd.state, cd.country, cd.country_of_passport
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

        COALESCE(ct.full_address, cmp.full_address)                         AS location_street_address,
        {{ concat('ct.full_address,ct.locality,ct.city,cmp.full_address,cmp.city') }} AS location_locality,
        COALESCE(ct.city, cmp.city)                                         AS location_metro,
        ''                                                                  AS location_region,
        ''                                                                  AS location_country,
        NULLIF(TRIM(ct.company), '')                                        AS location_fallback_label

    FROM {{ db }}.contact_data ct
    LEFT JOIN {{ db }}.company_data cmp
           ON cmp.slug = ct.company_slug
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
    -- Prefer explicit locality, then any geographic detail, then a labeled fallback
    COALESCE(
        NULLIF(
            {{ concat('location_locality,location_metro,location_region,location_country,location_street_address') }},
            ''
        ),
        NULLIF(location_fallback_label, ''),
        'Unknown Location'
    )                  AS location_locality,

    location_metro,
    location_region,
    location_country

FROM base_people