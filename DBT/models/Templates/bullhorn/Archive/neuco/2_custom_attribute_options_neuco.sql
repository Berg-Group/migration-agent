{{ config(
    materialized='table',
    alias='custom_attribute_options_neuco',
    tags = ["bullhorn"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('1_custom_attributes_neuco') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
notice_periods AS (
    SELECT DISTINCT c.customtext4
    FROM {{ var('source_database') }}.bh_usercontact c
    WHERE c.customtext4 IS NOT NULL AND c.customtext4 != ''
),
sources AS (
    SELECT DISTINCT c.customtext2
    FROM {{ var('source_database') }}.bh_clientcorporation c
    WHERE c.customtext2 IS NOT NULL AND c.customtext2 != ''
),
leads AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.customtext4, ';', ','), ',,', ','), ',', numbers.n)) AS customtext4
    FROM {{ var('source_database') }}.bh_clientcorporation c
    CROSS JOIN numbers
    WHERE c.customtext4 IS NOT NULL
        AND TRIM(c.customtext4) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.customtext4, ';', ','), ',,', ','), ',', numbers.n) != ''
),
company_statuses AS (
    SELECT DISTINCT 
        CASE 
            WHEN c.status IN ('Active Target (BD)') THEN 'Active Target (BD)'
            WHEN c.status IN ('Prospect (BD)', 'Engaged (BD)', 'Inactive Target (BD)') THEN 'Prospect (BD)'
            WHEN c.status IN ('Terms Agreed (AD)', 'Inactive Account (AD)') THEN 'Terms Agreed (AD)'
            WHEN c.status IN ('Active Account Tier 1 (AD)', 'Active Account Tier 2 (AD)', 'Active Account Tier 3 (AD)') THEN 'Active (AD)'
        END AS status
    FROM {{ var('source_database') }}.bh_clientcorporation c
    WHERE c.status IS NOT NULL AND c.status != '' AND c.status NOT IN ('Active', 'Archive', 'DNC', 'Prospect')
),
service_types AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(j.customtext3, ';', ','), ',,', ','), ',', numbers.n)) AS customtext3
    FROM {{ var('source_database') }}.bh_jobopportunity j
    CROSS JOIN numbers
    WHERE j.customtext3 IS NOT NULL
        AND TRIM(j.customtext3) != ''
        AND SPLIT_PART(REPLACE(REPLACE(j.customtext3, ';', ','), ',,', ','), ',', numbers.n) != ''
),
confidential_roles AS (
    SELECT DISTINCT j.customtext5
    FROM {{ var('source_database') }}.bh_jobopportunity j
    WHERE j.customtext5 IS NOT NULL AND j.customtext5 != ''
),
role_types AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.newvalue, ';', ','), ',,', ','), ',', numbers.n)) AS newvalue
    FROM {{ var('source_database') }}.bh_jobopportunity p
    INNER JOIN {{ var('source_database') }}.bh_jobpostingedithistory h ON h.jobpostingid = p.jobpostingid
    INNER JOIN {{ var('source_database') }}.bh_jobpostingedithistoryfieldchange c ON c.jobpostingedithistoryid = h.jobpostingedithistoryid
    CROSS JOIN numbers
    WHERE c.display = 'Role Type' 
        AND c.newvalue IS NOT NULL 
        AND c.newvalue != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.newvalue, ';', ','), ',,', ','), ',', numbers.n) != ''
),
company_sectors AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n)) AS businesssectorlist
    FROM {{ var('source_database') }}.bh_clientcorporation c
    CROSS JOIN numbers
    WHERE c.businesssectorlist IS NOT NULL 
        AND c.businesssectorlist != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
meeting_actions AS (
    SELECT alias, value
    FROM {{ ref('meeting_actions_bh') }}
),
combined_values AS (
    SELECT 'notice_period' AS alias, customtext4 AS value
    FROM notice_periods
    UNION ALL
    SELECT 'source' AS alias, customtext2 AS value
    FROM sources
    UNION ALL
    SELECT 'ad_bd_lead' AS alias, customtext4 AS value
    FROM leads
    UNION ALL
    SELECT 'service_type' AS alias, customtext3 AS value
    FROM service_types
    UNION ALL
    SELECT 'confidential_role' AS alias, customtext5 AS value
    FROM confidential_roles
    UNION ALL
    SELECT 'project_role_type' AS alias, newvalue AS value
    FROM role_types
    UNION ALL
    SELECT 'company_sector' AS alias, businesssectorlist AS value
    FROM company_sectors
    UNION ALL
    SELECT 'company_status' AS alias, status AS value
    FROM company_statuses
    UNION ALL
    SELECT alias, value
    FROM meeting_actions
)
SELECT
    cv.alias || '_' || value AS id,
    {{ atlas_uuid("cv.alias || value") }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    value AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY value ASC) AS position,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_values cv
INNER JOIN 
    internal_attributes ia ON ia.alias = cv.alias
ORDER BY
    atlas_attribute_id,
    position