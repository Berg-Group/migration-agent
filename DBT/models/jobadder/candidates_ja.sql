-- File: models/candidates_ja.sql

{{ config(
    materialized='table',
    alias='candidates_jobadder'
) }}

WITH base AS (
    SELECT
        TO_CHAR(s.datecreated::date, 'YYYY-MM-DD"T00:00:00"') AS created_at,
        COALESCE(
            TO_CHAR(s.dateupdated::date, 'YYYY-MM-DD"T00:00:00"'),
            TO_CHAR(s.datecreated::date, 'YYYY-MM-DD"T00:00:00"')
        ) AS updated_at,
        s.applicationid AS id,
        LOWER(
            SUBSTRING(MD5(s.applicationid::text || '{{ var("clientName") }}'), 1, 8) || '-' ||
            SUBSTRING(MD5(s.applicationid::text || '{{ var("clientName") }}'), 9, 4) || '-' ||
            SUBSTRING(MD5(s.applicationid::text || '{{ var("clientName") }}'), 13, 4) || '-' ||
            SUBSTRING(MD5(s.applicationid::text || '{{ var("clientName") }}'), 17, 4) || '-' ||
            SUBSTRING(MD5(s.applicationid::text || '{{ var("clientName") }}'), 21, 12)
        ) AS atlas_id,
        s.joborderid AS project_id,
        pj.atlas_id AS atlas_project_id,
        s.contactid AS person_id,
        p.atlas_id AS atlas_person_id,
        s.owneruserid AS created_by_id,
        COALESCE(uj.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        COALESCE(uj.atlas_id, '{{ var("master_id") }}') AS owner_id,
        CASE
    WHEN js.name IN (
        'Internal Interview',
        'A Possibility',
        'Unsuccessful - Visa',
        'Reference Checking',
        'Unsuccessful - Skills',
        'Withdrew application',
        'Applied',
        'Unsuccessful',
        'Left message to call',
        'Unsuccessful - Good candidate'
    ) THEN 'Added'
    WHEN js.name = 'Resume Submitted' THEN 'Presented'
    WHEN js.name IN (
        'Client Interview 1',
        'Client Interview 2',
        'Client Interview 3'
    ) THEN 'Client IV'
    WHEN js.name =  'Offered' THEN 'Offer'
    WHEN js.name IN (
        'Placed PERM',
        'Placed CONTRACT'
    ) THEN 'Hired'
    END AS status,
    'Candidate' AS class_type
    FROM {{ var('source_database') }}.jobapplication s
    LEFT JOIN "{{ this.schema }}".projects_ja pj
           ON s.joborderid = pj.id
    LEFT JOIN {{ref('1_people_ja')}} p
           ON s.contactid = p.id
    LEFT JOIN {{ var('source_database') }}.jobapplicationstatus js
           ON s.statusid = js.statusid
    LEFT JOIN "{{ this.schema }}".users_ja uj
           ON s.owneruserid = uj.id
)

SELECT *
FROM base
WHERE atlas_project_id NOTNULL
AND atlas_person_id NOTNULL 
