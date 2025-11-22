{{ config(
    materialized='table',
    alias='project_fees_ff',
    tags=["filefinder"]
) }}

WITH internal_fee_types AS (
    SELECT 
        name AS fee_type_name,
        agency_id,
        atlas_id AS fee_type_id
    FROM 
        {{ ref('15_fee_types_ff') }}
)
SELECT
    TRIM(COALESCE(a.estimatedfee::text, a.finalfee::text, a.feecomment::text)) || '_projected_' || a.idassignment::text AS id,
    {{ atlas_uuid("TRIM(COALESCE(a.estimatedfee::text, a.finalfee::text, a.feecomment::text)) || '_projected_' || a.idassignment::text") }} AS atlas_id,
    pf.id AS project_id,
    pf.atlas_id AS atlas_project_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
    NULL AS deleted_at,
    '{{ var("master_id") }}' AS created_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id,
    ift.fee_type_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS fee_date,
    TRIM(COALESCE(a.finalfee::text, a.estimatedfee::text)) AS amount,
    COALESCE(TRIM(c.value), 'USD') AS currency,
    'paid' AS project_fee_status,
    CASE 
        WHEN TRIM(COALESCE(a.feecomment, '')) != '' THEN 'Migrated Fee: ' || TRIM(a.feecomment)
        ELSE 'Migrated Fee'
    END AS notes,
    TRIM(COALESCE(a.finalfee::text, a.estimatedfee::text)) AS default_amount,
    COALESCE(TRIM(c.value), 'USD') AS agency_currency
FROM {{ var('source_database') }}."assignment" a 
INNER JOIN {{ this.schema }}.projects_ff pf ON pf.id = a.idassignment
INNER JOIN internal_fee_types ift ON ift.agency_id = '{{ var("agency_id") }}'
LEFT JOIN {{ var('source_database') }}.currency c ON c.idcurrency = a.idcurrency 
WHERE (a.baseestimatedfee IS NOT NULL AND a.baseestimatedfee <> 0) OR 
    (a.finalfee IS NOT NULL AND a.finalfee <> 0)