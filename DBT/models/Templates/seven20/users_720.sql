{{ config(
    materialized='table',
    alias='users',
    tags=["seven20"]
) }}


SELECT
    id,
    SUBSTRING(MD5(id::text), 1, 8) || '-' ||
    SUBSTRING(MD5(id::text), 9, 4) || '-' ||
    SUBSTRING(MD5(id::text), 13, 4) || '-' ||
    SUBSTRING(MD5(id::text), 17, 4) || '-' ||
    SUBSTRING(MD5(id::text), 21, 12) AS atlas_id,
    firstname || ' ' || lastname AS name,
    username AS email
FROM 
    {{ var('source_database') }}."user"
