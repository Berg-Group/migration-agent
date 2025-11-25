{{ config(
    materialized = 'table',
    alias        = 'companies_to_import',
    tags         = ['bullhorn']
) }}

SELECT
    c.clientcorporationid AS company_id
FROM
    {{ var("source_database") }}.bh_clientcorporation c
WHERE
    c.clientcorporationid IN (
        SELECT DISTINCT cl.clientcorporationid
        FROM {{ var("source_database") }}.bh_client cl
        INNER JOIN {{ var("source_database") }}.bh_usercomment uc
            ON uc.userid = cl.userid
    )
    OR c.clientcorporationid IN (
        SELECT DISTINCT j.clientcorporationid
        FROM {{ var("source_database") }}.bh_jobopportunity j
    )

