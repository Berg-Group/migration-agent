{{ config(
    materialized = 'table',
    alias        = 'project_fees_bh',
    tags=["bullhorn"]
) }}

WITH placements AS (
    SELECT
        pc.placementid,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        j.id AS project_id,
        j.atlas_id AS atlas_project_id, 
        pc.salary,
        pc.fee,
        pc.comments,
        pc.dateadded
    FROM {{ var('source_database') }}.bh_placement pc 
    INNER JOIN {{ ref('1_people_bh') }} p ON p.id = pc.userid 
    INNER JOIN {{ ref('11_projects_bh') }} j ON j.id = pc.jobpostingid
),
internal_fee_types AS (
    SELECT 
        name AS fee_type_name,
        agency_id,
        atlas_id AS fee_type_id
    FROM 
        {{ ref('15_fee_types_bh') }}
)
SELECT
    TRIM(pl.fee::text) || '_paid_' || pl.placementid::text AS id,
    {{ atlas_uuid("TRIM(pl.fee::text) || '_paid_' || pl.placementid::text") }} AS atlas_id,
    pl.project_id,
    pl.atlas_project_id,
    pl.person_id AS candidate_id,
    pl.atlas_person_id AS person_id,
    TO_CHAR(pl.dateadded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(pl.dateadded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    NULL AS deleted_at,
    '{{ var("master_id") }}' AS created_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id,
    ift.fee_type_id,
    TO_CHAR(pl.dateadded::timestamp(0), 'YYYY-MM-DD') AS fee_date,
    ROUND((pl.salary::numeric(18,2) * pl.fee::numeric(18,6)))::bigint AS amount,
    'GBP' AS currency,
    'paid' AS project_fee_status,
    CASE 
        WHEN TRIM(COALESCE(pl.comments::text, '')) != '' THEN TRIM(pl.comments::text)
        ELSE 'Migrated Placement Fee'
    END AS notes,
    ROUND((pl.salary::numeric(18,2) * pl.fee::numeric(18,6)))::bigint AS default_amount,
    'GBP' AS agency_currency,
    CASE WHEN pl.salary IS NOT NULL AND pl.salary <> 0 THEN 'placement' ELSE NULL END AS category,
    NULL AS start_date,
    CASE WHEN pl.salary IS NULL OR pl.salary = 0 THEN NULL ELSE pl.salary::bigint END AS payment_amount,
    CASE WHEN pl.salary IS NOT NULL AND pl.salary <> 0 THEN 'GBP' ELSE NULL END AS payment_currency
FROM placements pl
INNER JOIN internal_fee_types ift ON ift.agency_id = '{{ var("agency_id") }}'
WHERE pl.salary != '0'