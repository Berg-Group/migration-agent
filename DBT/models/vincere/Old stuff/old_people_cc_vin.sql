-- File: models/vincere/people_cc_vin.sql

{{ config(
    materialized='table',
    alias='people_cc_vincere'
) }}

WITH base AS (
    SELECT
        'cc' || {{ var('source_database') }}."contact".id::text AS id,
        lower(
            substring(md5(
                '{{ var('clientName') }}' || 
                {{ var('source_database') }}."contact".id::text || 
                COALESCE({{ var('source_database') }}."contact".email, to_char(current_timestamp, 'YYYYMMDDHH24MISS'))
            ), 1, 8) || '-' ||
            substring(md5(
                '{{ var('clientName') }}' || 
                {{ var('source_database') }}."contact".id::text || 
                COALESCE({{ var('source_database') }}."contact".email, to_char(current_timestamp, 'YYYYMMDDHH24MISS'))
            ), 9, 4) || '-' ||
            substring(md5(
                '{{ var('clientName') }}' || 
                {{ var('source_database') }}."contact".id::text || 
                COALESCE({{ var('source_database') }}."contact".email, to_char(current_timestamp, 'YYYYMMDDHH24MISS'))
            ), 13, 4) || '-' ||
            substring(md5(
                '{{ var('clientName') }}' || 
                {{ var('source_database') }}."contact".id::text || 
                COALESCE({{ var('source_database') }}."contact".email, to_char(current_timestamp, 'YYYYMMDDHH24MISS'))
            ), 17, 4) || '-' ||
            substring(md5(
                '{{ var('clientName') }}' || 
                {{ var('source_database') }}."contact".id::text || 
                COALESCE({{ var('source_database') }}."contact".email, to_char(current_timestamp, 'YYYYMMDDHH24MISS'))
            ), 21, 12)
        ) AS atlas_id,
        {{ var('source_database') }}."contact".first_name AS first_name,
        {{ var('source_database') }}."contact".last_name AS last_name,
        {{ var('source_database') }}."contact".user_account_id AS created_by_id,
        {{ var('source_database') }}."contact".user_account_id AS updated_by_id,
        '{{ var('agency_id') }}' AS agency_id,
        '2025-03-05T00:00:00' AS created_at,
        '2025-03-05T00:00:00' AS updated_at,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        '{{ var('created_by_id') }}' AS external_created_by_id,
        '{{ var('created_by_id') }}' AS external_updated_by_id,
        {{ var('source_database') }}."contact".current_location_id
    FROM 
        {{ var('source_database') }}."contact"
)

SELECT
    base.id,
    base.atlas_id,
    base.first_name,
    base.last_name,
    base.created_by_id,
    base.updated_by_id,
    base.agency_id,
    base.created_at,
    base.updated_at,
    base.responsiveness,
    base.candidate_status,
    base.external_created_by_id,
    base.external_updated_by_id,
    CASE
        WHEN pcl.address ILIKE '%connections%' THEN NULL
        ELSE pcl.address
    END AS location_locality,
    CASE
        WHEN pcl.country IS NOT NULL THEN pcl.country
        ELSE NULL
    END AS location_country,
    CASE
        WHEN pcl.post_code IS NOT NULL THEN pcl.post_code
        ELSE NULL
    END AS location_postal_code,
    CASE
        WHEN pcl.city IS NOT NULL AND pcl.city NOT ILIKE '%500+ connections%' THEN pcl.city
        ELSE NULL
    END AS location_region
FROM
    base
LEFT JOIN
    {{ var('source_database') }}."common_location" pcl
    ON base.current_location_id = pcl.id
