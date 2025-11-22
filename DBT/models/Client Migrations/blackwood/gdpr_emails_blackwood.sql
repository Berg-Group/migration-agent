{{ config(
    materialized='table',
    alias='gdpr_emails_blackwood',
    tags=["blackwood"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM {{ ref('people_blackwood') }}
)

SELECT 
    {{ atlas_uuid('cref::text || encr1::text') }} AS atlas_id,
    to_char(gdpremailsentdate, 'YYYY-MM-DD"T"00:00:00') AS created_at,
    to_char(gdpremailsentdate, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
    '{{var('agency_id')}}' AS agency_id,
    cg.cref AS person_id,
    atlas_person_id,
    '{{var('created_by_id')}}' AS atlas_sender_id,
    NULL AS email_address,
    NULL AS subject_html,
    NULL AS body_html,
    to_char(gdpremailsentdate, 'YYYY-MM-DD"T"00:00:00') AS planned_at,
    to_char(gdpremailsentdate, 'YYYY-MM-DD"T"00:00:00') AS sent_at,    
    NULL AS email_id
FROM 
    {{var('source_database')}}."candidate_gdpr" cg
INNER JOIN
    internal_persons ip ON ip.person_id = cg.cref
WHERE 
    gdprstatus = 1