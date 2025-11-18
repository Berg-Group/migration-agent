{{ config(
    materialized='table',
    alias='candidates_rect',
    tags=['recruitly']
) }}

WITH base AS (
    SELECT
        jp.pipeline_id AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || jp.pipeline_id::text") }} AS atlas_id,
        jp.job_id AS project_id,
        pr.atlas_id AS atlas_project_id,
        jp.candidate_id AS person_id,
        pe.atlas_id AS atlas_person_id,
        jp.createdby AS owner_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        {{ string_to_timestamp('jp.createdon') }} AS created_at,
        {{ string_to_timestamp('jp.modifiedon') }} AS updated_at,
        CASE
            WHEN jp.pipeline_status = 'Placed' OR c.status = 'Placed' THEN 'Hired'
            WHEN jp.pipeline_status = 'Interview' THEN 'Client IV'
            WHEN jp.pipeline_status = 'Offer' THEN 'Offer'
            WHEN jp.pipeline_status = 'Applied' THEN 'Added'
            ELSE 'Added'
        END AS status,
        CASE
            WHEN LOWER(COALESCE(jp.is_rejected, 'false')) = 'true' THEN
                CASE
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%unsuccessful%' THEN 'not_qualified'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%accepted%' THEN 'accepted_another_offer'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%found own job%' THEN 'accepted_another_offer'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%placed elsewhere%' THEN 'accepted_another_offer'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%accepted jumar%' THEN 'accepted_another_offer'
                    ELSE 'other'
                END
            ELSE NULL
        END AS rejection_reason,
        CASE
            WHEN LOWER(COALESCE(jp.is_rejected, 'false')) = 'true' THEN
                CASE
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%withdrew%' THEN 'self'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%unsuccessful%' THEN 'by_client'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%accepted%' THEN 'self'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%found own job%' THEN 'self'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%staying in own job%' THEN 'self'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%withdrawn offer%' THEN 'self'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%placed elsewhere%' THEN 'by_us'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%wasnt impressed%' THEN 'self'
                    WHEN LOWER(COALESCE(jp.reject_reason, '')) ILIKE '%accepted jumar%' THEN 'self'
                    ELSE 'by_us'
                END
            ELSE NULL
        END AS rejection_type,
        CASE
            WHEN LOWER(COALESCE(jp.is_rejected, 'false')) = 'true' THEN {{ string_to_timestamp('jp.modifiedon') }}
            ELSE NULL
        END AS rejected_at,
        CASE
            WHEN LOWER(COALESCE(jp.is_rejected, 'false')) = 'true' THEN COALESCE(u.atlas_id, '{{ var("master_id") }}')
            ELSE NULL
        END AS atlas_rejected_by_id,
        CASE
            WHEN jp.pipeline_status = 'Placed' OR c.status = 'Placed' THEN {{ string_to_timestamp('jp.modifiedon') }}
            ELSE NULL
        END AS hired_at,
        'Candidate' AS class_type
    FROM {{ var('source_database') }}.job_pipelines jp
    INNER JOIN {{ ref('7_projects_rect') }} pr ON pr.id = jp.job_id
    INNER JOIN {{ ref('2_people_rect') }} pe ON pe.id = jp.candidate_id
    INNER JOIN {{ var('source_database') }}.candidates c ON c.candidate_id = jp.candidate_id
    LEFT JOIN {{ ref('1_users_rect') }} u ON u.id = jp.createdby
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
    atlas_owner_id,
    class_type,
    status,
    rejected_at,
    rejection_type,
    rejection_reason,
    atlas_rejected_by_id,
    hired_at
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY atlas_project_id, atlas_person_id
            ORDER BY updated_at DESC, created_at DESC
        ) AS rn
    FROM base
) deduplicated
WHERE rn = 1


