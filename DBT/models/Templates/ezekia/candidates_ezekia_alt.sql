{{ config(
    materialized='table',
    alias='candidates_ezeloa'
) }}

WITH projects_map AS (
    SELECT
        p.id,
        p.atlas_id AS atlas_project_id
    FROM {{ ref('projects_ezekia') }} p
),

people_map AS (
    SELECT
        pe.id,
        pe.atlas_id AS atlas_person_id
    FROM {{ ref('people_ezekia') }} pe
),

users_map AS (
    SELECT
        u.name,
        u.atlas_id AS atlas_owner_id
    FROM {{ ref('users_ezekia') }} u
),

source_data AS (
    SELECT
        bc.id,

        -- MD5-based atlas_id
        LOWER(
            SUBSTRING(MD5('{{ var("clientName") }}' || bc.id::text), 1, 8)
            || '-' ||
            SUBSTRING(MD5('{{ var("clientName") }}' || bc.id::text), 9, 4)
            || '-' ||
            SUBSTRING(MD5('{{ var("clientName") }}' || bc.id::text), 13, 4)
            || '-' ||
            SUBSTRING(MD5('{{ var("clientName") }}' || bc.id::text), 17, 4)
            || '-' ||
            SUBSTRING(MD5('{{ var("clientName") }}' || bc.id::text), 21, 12)
        ) AS atlas_id,

        -- Convert created_at / updated_at to ISO8601 at midnight UTC
        TO_CHAR(DATE_TRUNC('day', bc.created_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"00:00:00Z') AS created_at,
        TO_CHAR(DATE_TRUNC('day', bc.updated_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"00:00:00Z') AS updated_at,

        bc.brief_id   AS project_id,
        bc.person_id,
        bc.added_by   AS owner_id
    FROM {{ var("source_database") }}.briefs_candidates bc
),

joined AS (
    SELECT
        sd.id,
        sd.atlas_id,
        sd.created_at,
        sd.updated_at,
        sd.project_id,
        pm.atlas_project_id,
        sd.person_id,
        pep.atlas_person_id,
        sd.owner_id,
        um.atlas_owner_id
    FROM source_data sd

    LEFT JOIN projects_map pm
           ON sd.project_id = pm.id

    LEFT JOIN people_map pep
           ON sd.person_id = pep.id

    -- If we truly match bc.added_by -> users_ezekia.name
    LEFT JOIN users_map um
           ON sd.owner_id = um.name
)

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    project_id,
    atlas_project_id,
    person_id,
    atlas_person_id,
    owner_id,
    /* If atlas_owner_id is null, default to var("master_id") */
    COALESCE(atlas_owner_id, '{{ var("master_id") }}') AS atlas_owner_id,
    'candidate' AS class_type
FROM joined
