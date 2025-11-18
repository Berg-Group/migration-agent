{{ config(
    materialized='table',
    alias='company_notes_rect',
    tags=['recruitly']
) }}

WITH source_companies AS (
    SELECT
        c.company_id AS company_id,
        co.atlas_id AS atlas_company_id,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at,
        NULL AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        NULLIF(TRIM({{ clean_html('c.internal_notes') }}), '') AS internal_text,
        NULLIF(TRIM({{ clean_html('c.benefits_package::varchar') }}), '') AS benefits_text,
        NULLIF(TRIM({{ clean_html('c.terms_agreed::varchar') }}), '') AS terms_text
    FROM {{ var('source_database') }}.companies c
    INNER JOIN {{ ref('4_companies_rect') }} co ON co.id = c.company_id
),
tasks AS (
    SELECT
        ('rect_task_' || t.task_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || r.id::text || '::task::' || t.task_id::text") }} AS atlas_id,
        r.id AS company_id,
        r.atlas_id AS atlas_company_id,
        NULL AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        (
            'Task: ' || TRIM(COALESCE(t.subject::varchar, '')) ||
            CASE 
                WHEN NULLIF(TRIM(COALESCE({{ clean_html('t.message') }}, '')), '') IS NOT NULL 
                    THEN chr(13) || chr(10) || TRIM({{ clean_html('t.message') }})
                ELSE ''
            END
        ) AS text,
        {{ string_to_timestamp('t.createdon') }} AS created_at,
        {{ string_to_timestamp('t.modifiedon') }} AS updated_at,
        'manual' AS type
    FROM {{ var('source_database') }}.tasks t
    INNER JOIN {{ ref('4_companies_rect') }} r
        ON r.id::text = REGEXP_REPLACE(TRIM(t.linked_to_records), '^COMPANY:', '')
    WHERE t.subject NOT ILIKE '%invoice%'
      AND t.linked_to_records ILIKE 'COMPANY:%'
),
notes AS (
    SELECT
        ('rect_internal_' || company_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || company_id::text || '::internal_notes'") }} AS atlas_id,
        company_id,
        atlas_company_id,
        created_by_id,
        created_by_atlas_id,
        'Internal notes:' || chr(13) || chr(10) || internal_text AS text,
        created_at,
        updated_at,
        'manual' AS type
    FROM source_companies
    WHERE internal_text IS NOT NULL

    UNION ALL

    SELECT
        ('rect_benefits_' || company_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || company_id::text || '::benefits_package'") }} AS atlas_id,
        company_id,
        atlas_company_id,
        created_by_id,
        created_by_atlas_id,
        'Benefits package:' || chr(13) || chr(10) || benefits_text AS text,
        created_at,
        updated_at,
        'manual' AS type
    FROM source_companies
    WHERE benefits_text IS NOT NULL

    UNION ALL

    SELECT
        ('rect_terms_' || company_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || company_id::text || '::terms_agreed'") }} AS atlas_id,
        company_id,
        atlas_company_id,
        created_by_id,
        created_by_atlas_id,
        'Terms agreed:' || chr(13) || chr(10) || terms_text AS text,
        created_at,
        updated_at,
        'manual' AS type
    FROM source_companies
    WHERE terms_text IS NOT NULL

    UNION ALL

    SELECT
        id,
        atlas_id,
        company_id,
        atlas_company_id,
        created_by_id,
        created_by_atlas_id,
        text,
        created_at,
        updated_at,
        type
    FROM tasks
)
SELECT
    id,
    atlas_id,
    company_id,
    atlas_company_id,
    created_by_id,
    created_by_atlas_id,
    text,
    created_at,
    updated_at,
    type
FROM notes