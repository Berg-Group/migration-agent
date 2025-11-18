{{ config(
    materialized = 'table',
    alias        = 'people_to_import',
    tags         = ['bullhorn']
) }}

SELECT
    bu.userid AS person_id
FROM
    {{ var("source_database") }}.bh_usercontact bu
WHERE
    bu.userid IN (
        SELECT DISTINCT u.userid
        FROM {{ var("source_database") }}.bh_usercomment u
    )
    OR 
    bu.userid IN (
        SELECT DISTINCT c.userid
        FROM {{ var("source_database") }}.bh_client c
    )
