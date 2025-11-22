{{ config(
    materialized='table',
    alias='company_identities_720',
    tags=["seven20"]
) }}

WITH internal_ids AS (
    SELECT DISTINCT 
        id AS company_id, 
        atlas_id AS atlas_company_id
    FROM "{{ this.schema }}"."companies"
),

cleaned_websites AS (
   SELECT
    TRIM(BOTH '/' FROM 
        REPLACE(REPLACE(REPLACE(website, 'https://', ''), 'http://', ''), 'www.', '')
    ) AS value,
        lower(
            substring(md5(
                regexp_replace(website, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 1, 8) || '-' ||
            substring(md5(
                regexp_replace(website, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 9, 4) || '-' ||
            substring(md5(
                regexp_replace(website, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 13, 4) || '-' ||
            substring(md5(
                regexp_replace(website, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 17, 4) || '-' ||
            substring(md5(
                regexp_replace(website, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 21, 12)
        ) AS id,
        
        createddate AS created_at,
        'website' AS type,
        id AS company_id,
        '{{ var('agency_id') }}' AS agency_id,
        true AS primary

    FROM {{ var('source_database') }}."account"
    WHERE website IS NOT NULL
),

cleaned_linkedin AS (
    SELECT 
    TRIM(BOTH '/' FROM 
        REPLACE(REPLACE(REPLACE(plaunch__linkedin__c, 'https://', ''), 'http://', ''), 'www.', '')
    ) AS value,


        lower(
            substring(md5(
                regexp_replace(plaunch__linkedin__c, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 1, 8) || '-' ||
            substring(md5(
                regexp_replace(plaunch__linkedin__c, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 9, 4) || '-' ||
            substring(md5(
                regexp_replace(plaunch__linkedin__c, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 13, 4) || '-' ||
            substring(md5(
                regexp_replace(plaunch__linkedin__c, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 17, 4) || '-' ||
            substring(md5(
                regexp_replace(plaunch__linkedin__c, '^https?://(www\.)?', '') || to_char(current_timestamp, 'YYYYMMDDHH24MISS')
            ), 21, 12)
        ) AS id,

        createddate AS created_at,
        'linkedin' AS type,
        id AS company_id,
        '{{ var('agency_id') }}' AS agency_id,
        false AS primary

    FROM {{ var('source_database') }}."account"
    WHERE plaunch__linkedin__c IS NOT NULL AND position('linkedin.com' in plaunch__linkedin__c) > 0
),

base AS (
    SELECT * FROM cleaned_websites
    UNION ALL 
    SELECT * FROM cleaned_linkedin
)

SELECT 
    b.*, 
    ii.atlas_company_id 
FROM base b
LEFT JOIN internal_ids ii USING (company_id)
