{{ config(
    materialized = 'table',
    alias        = 'company_identities_fr',
    tags         = ['seven20']
) }}

WITH internal_ids AS (
    SELECT DISTINCT
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM {{ ref('companies_fr') }}
),

cleaned_websites AS (
    SELECT
        trim(both '/' from replace(replace(replace(website, 'https://', ''), 'http://', ''), 'www.', '')) AS value,
        createddate AS created_at,
        'website' AS type,
        id AS company_id,
        '{{ var("agency_id") }}' AS agency_id,
        true AS primary
    FROM {{ var('source_database') }}."account"
    WHERE website IS NOT NULL
      AND trim(website) <> ''
      AND isdeleted = 0
),

ranked AS (
    SELECT
        cw.*,
        row_number() over (partition by value order by created_at) AS rn
    FROM cleaned_websites cw
),

base AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)

SELECT
    b.*,
    ii.atlas_company_id,
    {{ atlas_uuid('value') }} AS atlas_id
FROM base b
LEFT JOIN internal_ids ii USING (company_id)