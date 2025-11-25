{{ config(
    materialized='table',
    alias='agencies_forgetalent',
    tags=["forgetalent"]
) }}

SELECT '{{var('agency_id')}}' AS id,
       '{{var('clientName')}}' AS name,
       to_char('{{var('date')}}'::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
       to_char('{{var('date')}}'::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
       '{{var('domain')}}' AS domain 