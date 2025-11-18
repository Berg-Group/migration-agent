{{ config(
    materialized = 'table',
    alias        = 'fee_types_bh',
    tags=["bullhorn"]
) }}

SELECT
    {{ atlas_uuid("'" ~ var('agency_id') ~ "' || 'fee_type_projected'") }} AS atlas_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    NULL AS deleted_at,
    'Projected' AS name,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id,
    'fee' AS project_fee_type