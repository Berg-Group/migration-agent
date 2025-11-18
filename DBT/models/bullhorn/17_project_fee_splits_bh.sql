{{ config(
    materialized = 'table',
    alias        = 'project_fee_splits_bh',
    tags         = ['bullhorn']
) }}

WITH base AS (
    SELECT 
        c.commissionid AS id,
        p.jobpostingid AS project_id,
        bu.email AS bh_email,
        c.commissionpercentage
    FROM {{ var('source_database') }}.bh_commission c
    INNER JOIN {{ var('source_database') }}.bh_placement p ON p.placementid = c.placementid 
    INNER JOIN {{ var('source_database') }}.bh_usercontact bu ON bu.userid = c.userid 
),
mapped_users AS (
    SELECT 
        b.id,
        b.project_id,
        u.atlas_id AS atlas_fee_earner_id,
        b.commissionpercentage
    FROM base b
    INNER JOIN {{ ref('0_users_bh') }} u ON LOWER(TRIM(u.email)) = LOWER(TRIM(b.bh_email))
),
mapped_projects AS (
    SELECT 
        mu.id,
        p.atlas_id AS atlas_project_id,
        mu.atlas_fee_earner_id,
        mu.commissionpercentage
    FROM mapped_users mu
    INNER JOIN {{ ref('10_projects_bh') }} p ON p.id = mu.project_id
)
SELECT 
    id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || id::text") }} AS atlas_id,
    atlas_project_id,
    atlas_fee_earner_id,
    '{{ var("master_id") }}' AS atlas_created_by_id,
    ift.atlas_id AS atlas_fee_type_id,
    (commissionpercentage::numeric(18,6) * 100)::numeric(18,2) AS share,
    '{{ var("agency_id") }}' AS agency_id
FROM mapped_projects
INNER JOIN {{ ref('14_fee_types_bh') }} ift ON ift.agency_id = '{{ var("agency_id") }}'