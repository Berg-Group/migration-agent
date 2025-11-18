-- File: models/intercity/companies_ja.sql

{{ config(
    materialized='table',
    alias='companies_jobadder'
) }}

WITH base AS (
    SELECT
        {{ var('source_database') }}."company".companyid AS id,
        lower(
            substring(md5({{ var('source_database') }}."company".companyid::text || '{{ var('clientName') }}'), 1, 8) || '-' ||
            substring(md5({{ var('source_database') }}."company".companyid::text || '{{ var('clientName') }}'), 9, 4) || '-' ||
            substring(md5({{ var('source_database') }}."company".companyid::text || '{{ var('clientName') }}'), 13, 4) || '-' ||
            substring(md5({{ var('source_database') }}."company".companyid::text || '{{ var('clientName') }}'), 17, 4) || '-' ||
            substring(md5({{ var('source_database') }}."company".companyid::text || '{{ var('clientName') }}'), 21, 12)
        ) AS atlas_id,
        to_char({{ var('source_database') }}."company".datecreated, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
        to_char({{ var('source_database') }}."company".dateupdated, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
        {{ var('source_database') }}."company".name AS name,
        'none' AS relationship
    FROM 
        {{ var('source_database') }}."company"
)

SELECT *
FROM base
