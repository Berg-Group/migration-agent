-- File: models/vincere/files_vin.sql

{{ config(
    materialized='table',
    alias='files_vincere'
) }}

WITH base AS (
    SELECT
        s.id AS id,
        -- Generate a UUID from the MD5 hash of the id
        lower(
            substring(md5(s.id::text), 1, 8) || '-' ||
            substring(md5(s.id::text), 9, 4) || '-' ||
            substring(md5(s.id::text), 13, 4) || '-' ||
            substring(md5(s.id::text), 17, 4) || '-' ||
            substring(md5(s.id::text), 21, 12)
        ) AS atlas_id,
        s.uploaded_filename AS file_name,
        regexp_replace(s.uploaded_filename, '^.*\.([^.]+)$', '\\1') AS ext,
        s.filesize AS size,
        'other' AS type,
        -- Transform insert_timestamp to ISO8601 format once using concatenation
        extract(year FROM s.insert_timestamp::timestamp)::text || '-' ||
        lpad(extract(month FROM s.insert_timestamp::timestamp)::text, 2, '0') || '-' ||
        lpad(extract(day FROM s.insert_timestamp::timestamp)::text, 2, '0') || 'T' ||
        lpad(extract(hour FROM s.insert_timestamp::timestamp)::text, 2, '0') || ':' ||
        lpad(extract(minute FROM s.insert_timestamp::timestamp)::text, 2, '0') || ':' ||
        lpad(extract(second FROM s.insert_timestamp::timestamp)::text, 2, '0') || 'Z' AS iso8601_timestamp,
        s.saved_filename AS path,
        s.candidate_id AS person_id
    FROM 
        {{ var('source_database') }}."public_candidate_document" s
    WHERE
        s.candidate_id IS NOT NULL
)

SELECT
    base.id,
    base.atlas_id,
    base.file_name,
    base.ext,
    base.size,
    base.type,
    base.iso8601_timestamp AS created_at,
    base.iso8601_timestamp AS updated_at,
    base.iso8601_timestamp AS received_at,
    base.path,
    base.person_id,
    pv.external_id AS atlas_person_id
FROM
    base
LEFT JOIN
    "{{ this.database }}"."{{ this.schema }}"."people_vincere" pv
    ON pv.id = base.person_id
