-- File: models/vincere/fix_cand_cont_dupe.sql

{{ config(
    materialized='table',
    alias='cand_cont_map'
) }}

WITH source_data AS (
    SELECT
        {{ var('source_database') }}."candidate".id AS candidate_id,
        'cc' || {{ var('source_database') }}."candidate".contact_id AS company_id -- Prefix company_id with cc
    FROM
        {{ var('source_database') }}."candidate"
    WHERE
        {{ var('source_database') }}."candidate".contact_id IS NOT NULL
)

SELECT
    candidate_id,
    company_id, -- Already prefixed with cc in source_data
    pv.atlas_id AS atlas_candidate_id,
    pcv.atlas_id AS atlas_cc_id
FROM
    source_data
LEFT JOIN "{{ this.database }}"."{{ this.schema }}"."people_vincere" pv
    ON pv.id = candidate_id
LEFT JOIN "{{ this.database }}"."{{ this.schema }}"."people_cc_vincere" pcv
    ON pcv.id = company_id