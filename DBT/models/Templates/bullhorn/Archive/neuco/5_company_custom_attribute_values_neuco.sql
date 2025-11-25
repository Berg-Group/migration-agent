{{ config(
    materialized='table',
    alias='company_custom_attribute_values_neuco',
    tags = ["bullhorn"]
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ ref('3_companies_bh') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_neuco') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_neuco') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'company'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
source_mappings AS (
    SELECT DISTINCT
        c.ClientCorporationID AS company_id,
        TRIM(c.customtext2) AS source_value
    FROM {{ var('source_database') }}.bh_clientcorporation c
    WHERE c.customtext2 IS NOT NULL 
      AND TRIM(c.customtext2) != ''
),
lead_mappings AS (
    SELECT DISTINCT
        c.ClientCorporationID AS company_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.customtext4, ';', ','), ',,', ','), ',', numbers.n)) AS lead_value
    FROM {{ var('source_database') }}.bh_clientcorporation c
    CROSS JOIN (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) numbers
    WHERE c.customtext4 IS NOT NULL
        AND TRIM(c.customtext4) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.customtext4, ';', ','), ',,', ','), ',', numbers.n) != ''
),
company_status_mappings AS (
    SELECT DISTINCT
        c.ClientCorporationID AS company_id,
        CASE 
            WHEN c.status IN ('Active Target (BD)') THEN 'Active Target (BD)'
            WHEN c.status IN ('Prospect (BD)', 'Engaged (BD)', 'Inactive Target (BD)') THEN 'Prospect (BD)'
            WHEN c.status IN ('Terms Agreed (AD)', 'Inactive Account (AD)') THEN 'Terms Agreed (AD)'
            WHEN c.status IN ('Active Account Tier 1 (AD)', 'Active Account Tier 2 (AD)', 'Active Account Tier 3 (AD)') THEN 'Active (AD)'
        END AS company_status_value
    FROM {{ var('source_database') }}.bh_clientcorporation c
    WHERE c.status IS NOT NULL 
      AND TRIM(c.status) != ''
      AND c.status NOT IN ('Active', 'Archive', 'DNC', 'Prospect')
),
company_sector_mappings AS (
    SELECT DISTINCT
        c.ClientCorporationID AS company_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n)) AS sector_value
    FROM {{ var('source_database') }}.bh_clientcorporation c
    CROSS JOIN numbers
    WHERE c.businesssectorlist IS NOT NULL 
      AND c.businesssectorlist != ''
      AND SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_mappings AS (
    SELECT 
        company_id,
        'source'::text AS attribute_type,
        source_value AS value
    FROM source_mappings
    UNION ALL
    SELECT 
        company_id,
        'ad_bd_lead'::text AS attribute_type,
        lead_value AS value
    FROM lead_mappings
    UNION ALL
    SELECT 
        company_id,
        'company_status'::text AS attribute_type,
        company_status_value AS value
    FROM company_status_mappings
    UNION ALL
    SELECT 
        company_id,
        'company_sector'::text AS attribute_type,
        sector_value AS value
    FROM company_sector_mappings
)
SELECT DISTINCT
    {{ atlas_uuid('ic.company_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ic.company_id,
    ic.atlas_company_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_mappings cm
INNER JOIN 
    internal_companies ic ON ic.company_id = cm.company_id
INNER JOIN 
    internal_options io ON io.option_value = cm.value AND io.attribute_type = cm.attribute_type
ORDER BY
    ic.company_id,
    io.atlas_attribute_id 