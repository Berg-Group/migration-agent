-- File: models/vincere/project_stages_vin.sql

{{ config(
    materialized='table',
    alias='project_stages_vincere'
) }}

WITH projects AS (
    SELECT
        s.id AS project_id,
        s.atlas_id AS atlas_project_id,
        s.created_at,
        '{{ var("agency_id") }}' AS agency_id
    FROM
        {{ ref('8_projects_vin') }} s
),

stages AS (
    SELECT
        p.project_id,
        p.atlas_project_id,
        p.created_at,
        p.agency_id,
        -- First stage
        'sourcing' AS name,
        'sourcing' AS type,
        1 AS position,
        'internal_selection' AS phase
    FROM projects p

    UNION ALL

    SELECT
        p.project_id,
        p.atlas_project_id,
        p.created_at,
        p.agency_id,
        -- Second stage
        'hired' AS name,
        'hired' AS type,
        2 AS position,
        'late_stage' AS phase
    FROM projects p
),

final AS (
    SELECT
        -- Generate d.id using md5 based on project_id, name, and current system time
        lower(
            substring(md5(p.project_id::text || s.name || CURRENT_TIMESTAMP::text), 1, 8) || '-' ||
            substring(md5(p.project_id::text || s.name || CURRENT_TIMESTAMP::text), 9, 4) || '-' ||
            substring(md5(p.project_id::text || s.name || CURRENT_TIMESTAMP::text), 13, 4) || '-' ||
            substring(md5(p.project_id::text || s.name || CURRENT_TIMESTAMP::text), 17, 4) || '-' ||
            substring(md5(p.project_id::text || s.name || CURRENT_TIMESTAMP::text), 21, 12)
        ) AS id,

        p.created_at AS created_at,
        p.created_at AS updated_at,
        p.agency_id,
        p.project_id,
        p.atlas_project_id,
        'CandidateStage' AS class_type,
        s.name,
        s.type,
        s.position,
        s.phase

    FROM stages s
    JOIN projects p ON p.project_id = s.project_id
)

SELECT *
FROM final
