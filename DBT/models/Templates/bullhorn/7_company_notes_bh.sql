{{ config(
    materialized = 'table',
    alias        = 'company_notes_bh',
    tags         = ['bullhorn']
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM {{ ref('3_companies_bh') }}
),
source_notes AS (
    SELECT
        regexp_replace(
            bcc.notes,
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS text,
        bcc."ClientCorporationID" AS company_id,
        TO_CHAR(bcc."DateAdded"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(bcc."DateLastModified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM {{ var('source_database') }}."bh_clientcorporation" bcc
    WHERE bcc.notes IS NOT NULL
      AND TRIM(bcc.notes) <> ''
)
SELECT
    sn.company_id AS id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || 'companynote' || sn.company_id::text") }} AS atlas_id,
    sn.company_id,
    ic.atlas_company_id,
    sn.text AS text,
    'manual' AS type,
    sn.created_at,
    sn.updated_at,
    NULL AS created_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id
FROM source_notes sn
INNER JOIN internal_companies ic USING (company_id)