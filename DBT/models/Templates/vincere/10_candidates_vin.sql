{{ config(materialized='table', alias='candidates_vincere') }}

WITH src AS (
    SELECT
        s.id,
        s.position_description_id,
        s.candidate_id,
        s.created_by,
        s.insert_timestamp,
        s.modified_date,
        s.rejected_date,
        DATE_TRUNC(
            'second',
            GREATEST(
                s.associated_date, s.applied_date, s.sent_date,
                s.interview1_date, s.interview2_date, s.interview3_date,
                s.interview4_date, s.interview5_date, s.offer_date, s.hire_date
            )::timestamp
        )                                                          AS event_ts,
        CASE
            WHEN s.hire_date IS NOT NULL THEN 'Hired'
            WHEN s.offer_date IS NOT NULL THEN 'Offer'
            WHEN s.interview5_date IS NOT NULL OR s.interview4_date IS NOT NULL OR
                 s.interview3_date IS NOT NULL OR s.interview2_date IS NOT NULL OR
                 s.interview1_date IS NOT NULL THEN 'Client IV'
            WHEN s.sent_date IS NOT NULL THEN 'Presented'
            ELSE 'Added'
        END                                                        AS calc_status,
        ROW_NUMBER() OVER (
            PARTITION BY s.position_description_id, s.candidate_id
            ORDER BY
                DATE_TRUNC(
                    'second',
                    GREATEST(
                        s.associated_date, s.applied_date, s.sent_date,
                        s.interview1_date, s.interview2_date, s.interview3_date,
                        s.interview4_date, s.interview5_date, s.offer_date, s.hire_date
                    )::timestamp
                ) DESC,
                s.modified_date DESC NULLS LAST,
                s.insert_timestamp DESC NULLS LAST
        )                                                          AS rn
    FROM {{ var('source_database') }}."public_position_candidate" s
),

internal_users AS (
    SELECT id, atlas_id
    FROM {{ ref('users_vin') }}
)

SELECT
    TO_CHAR(src.insert_timestamp::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(src.modified_date::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS')    AS updated_at,
    src.id,
    lower(
       substring(md5('chaseglobal' || 'vincere_candidate_' || '{{ var("clientName") }}' || src.id || '_' || src.candidate_id::text),  1,  8) || '-' ||
       substring(md5('chaseglobal' || 'vincere_candidate_' || '{{ var("clientName") }}' || src.id || '_' || src.candidate_id::text),  9,  4) || '-' ||
       substring(md5('chaseglobal' || 'vincere_candidate_' || '{{ var("clientName") }}' || src.id || '_' || src.candidate_id::text), 13,  4) || '-' ||
       substring(md5('chaseglobal' || 'vincere_candidate_' || '{{ var("clientName") }}' || src.id || '_' || src.candidate_id::text), 17,  4) || '-' ||
       substring(md5('chaseglobal' || 'vincere_candidate_' || '{{ var("clientName") }}' || src.id || '_' || src.candidate_id::text), 21, 12)
    )                                                                 AS atlas_id,
    '{{ var("agency_id") }}'                                          AS agency_id,
    src.position_description_id                                       AS project_id,
    pv.atlas_id                                                       AS atlas_project_id,
    src.candidate_id                                                  AS person_id,
    pe.atlas_id                                                       AS atlas_person_id,
    src.created_by::varchar                                           AS owner_id,
    COALESCE(iu.atlas_id,'{{ var("master_id") }}')                    AS atlas_owner_id,
    '{{ var("master_id") }}'                                          AS created_by_atlas_id,
    'Candidate'                                                       AS class_type,
    src.calc_status                                                   AS status,
    CASE
        WHEN src.rejected_date IS NOT NULL
        THEN TO_CHAR(DATE_TRUNC('day',src.rejected_date::timestamp),'YYYY-MM-DD"T00:00:00"')
    END                                                               AS rejected_at,
    CASE WHEN src.rejected_date IS NOT NULL THEN 'by_us'  END         AS rejection_type,
    CASE WHEN src.rejected_date IS NOT NULL THEN 'other'  END         AS rejection_reason
FROM src
JOIN {{ ref('1_people_vincere') }}           pe ON src.candidate_id          = pe.id
LEFT JOIN {{ ref('8_projects_vin') }}         pv ON src.position_description_id = pv.id
LEFT JOIN internal_users                      iu ON iu.id                     = src.created_by
WHERE src.rn = 1