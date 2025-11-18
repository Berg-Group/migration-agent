-- models/vincere/company_identities_vin.sql
{{ config(materialized = 'table', alias = 'company_identities_vincere') }}

WITH internal_ids AS (
    SELECT DISTINCT id AS company_id,
           atlas_id  AS atlas_company_id
    FROM {{ref('3_companies_vin')}}
),

cleaned_websites AS (
    SELECT
        REGEXP_REPLACE(REGEXP_REPLACE(pc.website,'^https?://',''),'^www\\.', '') AS value,
        {{ atlas_uuid("'{{ var(\"clientName\") }}' || pc.website") }} AS atlas_id,
        to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'website'              AS type,
        pc.id                  AS company_id,
        '{{ var("agency_id") }}' AS agency_id,
        TRUE                   AS is_primary
    FROM {{ var('source_database') }}."public_company" pc
    WHERE pc.website IS NOT NULL AND TRIM(pc.website) <> ''
),

cleaned_linkedin AS (
    SELECT
        REGEXP_REPLACE(REGEXP_REPLACE(pc.url_linkedin,'^https?://',''),'^www\\.', '') AS value,
        {{ atlas_uuid(" '{{ var(\"clientName\") }}' || pc.url_linkedin") }} AS atlas_id,
        to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')  AS created_at,
        to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')  AS updated_at,
        'linkedin'             AS type,
        pc.id                  AS company_id,
        '{{ var("agency_id") }}' AS agency_id,
        FALSE                  AS is_primary
    FROM {{ var('source_database') }}."public_company" pc
    WHERE pc.url_linkedin IS NOT NULL
      AND POSITION('linkedin.com' IN pc.url_linkedin) > 0
),

base AS (
    SELECT * FROM cleaned_websites
    UNION ALL
    SELECT * FROM cleaned_linkedin
),

deduped_values AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY value 
                ORDER BY 
                    -- First prioritize by type (website > linkedin)
                    CASE WHEN type = 'website' THEN 1 ELSE 2 END,
                    -- Then by is_primary flag
                    CASE WHEN is_primary THEN 1 ELSE 2 END
            ) AS rn
        FROM base
    ) t
    WHERE rn = 1
)

SELECT  d.*,
        ii.atlas_company_id
FROM    deduped_values d
INNER JOIN internal_ids ii USING (company_id)
