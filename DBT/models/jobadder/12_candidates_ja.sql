-- File: models/candidates_ja.sql

{{ config(
    materialized='table',
    alias='candidates_ja'
) }}

WITH base AS (
    SELECT
        TO_CHAR(s.datecreated::date, 'YYYY-MM-DD"T00:00:00"') AS created_at,
        COALESCE(
            TO_CHAR(s.dateupdated::date, 'YYYY-MM-DD"T00:00:00"'),
            TO_CHAR(s.datecreated::date, 'YYYY-MM-DD"T00:00:00"')
        ) AS updated_at,
        s.applicationid AS id,
        s.applicationid::text || '{{ var("clientName") }}' AS uuid_input,
        s.joborderid AS project_id,
        pj.atlas_id AS atlas_project_id,
        s.contactid AS person_id,
        p.atlas_id AS atlas_person_id,
        s.owneruserid AS owner_id,
        COALESCE(uj.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        'Candidate' AS class_type,
        CASE 
        WHEN js.name = 'Submitted to client' THEN 'Presented'
        WHEN js.name IN ('Client interview 1', 'Client interview 2', 'Client Interview 3', 'Unsuccessful', 'Unsuccessful - Good candidate', 'Unsuccessful - Skills/Experience') THEN 'Client IV'
        when js.name in  ('Offered') then 'Offer'
        WHEN js.name ILIKE '%placed%' THEN 'Hired'
        ELSE 'Added'
    END AS status,
    CASE
        WHEN js.rejected = true THEN TO_CHAR(s.dateupdated::date, 'YYYY-MM-DD"T00:00:00"')
        ELSE NULL
    END AS rejected_at,
    CASE
        WHEN js.rejected = true THEN 'by_us'
        ELSE NULL
    END AS rejection_type,
    CASE
        WHEN js.rejected = true THEN 'other'
        ELSE NULL
    END AS rejection_reason,
    CASE WHEN js.rejected = TRUE THEN COALESCE(uj.atlas_id, '{{ var("master_id") }}')
        ELSE NULL END AS rejected_by_atlas_id

FROM {{ var('source_database') }}.jobapplication s
INNER JOIN {{ref('10_projects_ja')}} pj
       ON s.joborderid = pj.id
LEINNERFT JOIN {{ref('1_people_ja')}} p
       ON s.contactid = p.id
LEFT JOIN {{ var('source_database') }}.jobapplicationstatus js
       ON s.statusid = js.statusid
LEFT JOIN {{ref('users_ja')}} uj
       ON s.owneruserid = uj.id
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY atlas_project_id, atlas_person_id 
            ORDER BY updated_at DESC, created_at DESC, id DESC
        ) AS rn
    FROM base
    WHERE project_id IS NOT NULL
)

SELECT
    id,
    {{ atlas_uuid('uuid_input') }} AS atlas_id,
    created_at,
    updated_at,
    project_id,
    atlas_project_id,
    person_id,
    atlas_person_id,
    owner_id,
    atlas_owner_id,
    class_type,
    status,
    rejected_at,
    rejection_type,
    rejection_reason,
    rejected_by_atlas_id
FROM deduplicated
WHERE rn = 1
