{{ config(
    materialized = 'table',
    alias        = 'people_dupes_bh_2',
    tags         = ['bullhorn']
) }}

SELECT 
    b.userid AS contact_id, 
    b.linkeduserid AS candidate_id 
FROM 
    {{ var("source_database") }}.bh_usercontact b 
WHERE 
    b.linkeduserid IS NOT NULL
    AND b.userid IN (
        SELECT DISTINCT b.userid 
        FROM {{ var("source_database") }}.bh_client b
    )