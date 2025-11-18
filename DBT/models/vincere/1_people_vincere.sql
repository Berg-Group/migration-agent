{{ config(materialized = 'table', alias = 'people_vincere') }}

WITH candidates AS (
    SELECT
        c.id::text                                   AS id,
        c.id::text                                   AS original_candidate_id,
        NULL                                         AS original_contact_id,
        ('{{ var("agency_id") }}' || c.id::text || c.email) AS uuid_input,
        c.first_name,
        c.last_name,
        c.user_account_id                            AS created_by_id,
        c.user_account_id                            AS updated_by_id,
        '{{ var("agency_id") }}'                     AS agency_id,
        TO_CHAR(date_trunc('day', c.insert_timestamp::timestamp),'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'active'                                     AS responsiveness,
        'regular'                                    AS candidate_status,
        '{{ var("created_by_id") }}'                 AS created_by_atlas_id,
        '{{ var("created_by_id") }}'                 AS updated_by_atlas_id,
        c.external_id                                AS link_id,
        COALESCE(c.external_id,'candidate')          AS link,
        COALESCE(c.current_location_id,c.personal_location_id) AS current_location_id,
        NULL                                         AS location_raw,
        COALESCE(cc.id::text,'')                     AS company_contact_id,
        {{clean_html('note_backup')}} as overview 
    FROM {{ var("source_database") }}.public_candidate c
    LEFT JOIN {{ var("source_database") }}.public_contact cc
           ON cc.external_id = c.external_id
          AND cc.deleted_timestamp IS NULL
    WHERE c.deleted_reason IS NULL
      AND c.deleted_timestamp IS NULL
),

contact_loc_choice AS (
    SELECT contact_id,
           MIN(company_location_id) AS company_location_id
    FROM {{ var("source_database") }}.public_contact_location
    GROUP BY contact_id
),

non_matched_contacts AS (
    SELECT
        ('cont' || cc.id::text)                      AS id,
        NULL                                         AS original_candidate_id,
        cc.id::text                                  AS original_contact_id,
        ('{{ var("clientName") }}' || cc.id::text || COALESCE(cc.email,TO_CHAR(current_timestamp,'YYYYMMDDHH24MISS'))) AS uuid_input,
        cc.first_name,
        cc.last_name,
        cc.user_account_id                           AS created_by_id,
        cc.user_account_id                           AS updated_by_id,
        '{{ var("agency_id") }}'                     AS agency_id,
        TO_CHAR(date_trunc('day', cc.insert_timestamp::timestamp),'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'active'                                     AS responsiveness,
        'regular'                                    AS candidate_status,
        '{{ var("created_by_id") }}'                 AS created_by_atlas_id,
        '{{ var("created_by_id") }}'                 AS updated_by_atlas_id,
        cc.external_id                               AS link_id,
        COALESCE(cc.external_id,'contact')           AS link,
        cc.current_location_id,
        TRIM(BOTH ',. ' FROM COALESCE(pcl.address,pcl.location_name,pcl.country,pcl.address_line1)) AS location_raw,
        cc.id::text                                  AS company_contact_id,
        {{clean_html('note_backup')}} as overview 
    FROM {{ var("source_database") }}.public_contact cc
    LEFT JOIN {{ var("source_database") }}.public_candidate pc
           ON pc.external_id = cc.external_id
          AND pc.deleted_reason IS NULL
          AND pc.deleted_timestamp IS NULL
    LEFT JOIN contact_loc_choice cl
           ON cl.contact_id = cc.id
    LEFT JOIN {{ var("source_database") }}.public_company_location pcl
           ON pcl.id = cl.company_location_id
    WHERE cc.deleted_timestamp IS NULL
      AND (cc.external_id IS NULL OR pc.id IS NULL)
),

base AS (
    SELECT * FROM candidates
    UNION ALL
    SELECT * FROM non_matched_contacts
),

locations AS (
    SELECT
        id                                        AS location_id,
        TRIM(BOTH ',. ' FROM address)             AS loc_locality,
        TRIM(BOTH ',. ' FROM city)                AS loc_city,
        TRIM(BOTH ',. ' FROM country)             AS loc_country
    FROM {{ var("source_database") }}.public_common_location
)

SELECT
    base.id,
    base.original_candidate_id,
    base.original_contact_id,
    {{ atlas_uuid('base.uuid_input') }}            AS atlas_id,
    base.first_name,
    base.last_name,
    base.created_by_id,
    base.updated_by_id,
    base.agency_id,
    base.created_at,
    base.updated_at,
    base.responsiveness,
    base.candidate_status,
    base.created_by_atlas_id,
    base.updated_by_atlas_id,
    base.link_id,
    base.link,
    base.company_contact_id,
    TRIM(
        BOTH ', ' FROM
            COALESCE(base.location_raw,'') ||
            CASE WHEN COALESCE(base.location_raw,'') <> '' 
                 AND COALESCE(locations.loc_city,'') <> '' THEN ', ' ELSE '' END ||
            COALESCE(locations.loc_city,'') ||
            CASE WHEN (COALESCE(base.location_raw,'') <> '' 
                       OR COALESCE(locations.loc_city,'') <> '')
                 AND COALESCE(locations.loc_country,'') <> '' THEN ', ' ELSE '' END ||
            COALESCE(locations.loc_country,'')
    ) AS location_locality,
    base.overview
FROM base
LEFT JOIN locations
       ON base.current_location_id = locations.location_id