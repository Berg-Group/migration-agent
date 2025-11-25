{{ config(
    materialized='table',
    alias='company_relationships_ff',
    tags=["filefinder"]
) }}

SELECT
    {{ atlas_uuid("'parent_' || p.atlas_id || '_' || s.atlas_id") }} as atlas_id,
    s.id as subsidiary_id,
    p.id as company_id,
    s.atlas_id as atlas_subsidiary_id,
    p.atlas_id as atlas_company_id,
    'parent' as relationship_type,
    TO_CHAR(c.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
    TO_CHAR(c.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
    u.id AS created_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id
FROM {{ var('source_database') }}.Company c
INNER JOIN {{ this.schema }}.companies_ff s ON s.id = c.idCompany
INNER JOIN {{ this.schema }}.companies_ff p ON p.id = c.parentid
LEFT JOIN {{ this.schema }}.users_ff u ON LOWER(u.name) = LOWER(c.createdby)