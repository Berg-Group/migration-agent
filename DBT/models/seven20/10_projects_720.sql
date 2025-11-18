{{ config(
    materialized='table',
    alias='projects_720',
    tags=["seven20"]
) }}

WITH source_projects AS (
    SELECT
        j.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || j.id") }} AS atlas_id,
        j.name AS job_role,
        regexp_replace(
            j.seven20__client_description__c,
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS description,
        CASE
            WHEN j.engagement_type__c IN ('Full Time') THEN 'full_time'
            WHEN j.engagement_type__c IN ('Part Time') THEN 'contract'
            ELSE 'full_time'
        END AS contract_type,
        CASE 
            WHEN j.seven20__job_closed__c = 1 THEN 'closed'
            WHEN j.seven20__status__c IN ('Lost', 'Placed') OR p.seven20__job__c IS NOT NULL THEN 'closed' 
            WHEN j.seven20__status__c IN ('Interview', 'CV', 'No CVs', 'Offer') THEN 'active' 
        END AS state,
        CASE 
            WHEN j.seven20__status__c = 'Placed' OR p.seven20__job__c IS NOT NULL THEN 'won' 
            WHEN j.seven20__status__c = 'Lost' THEN 'worked_lost'
            WHEN j.seven20__job_closed__c = 1 AND j.seven20__status__c NOT IN ('Placed', 'Lost') AND p.seven20__job__c IS NULL THEN 'worked_lost'
        END AS close_reason,
        CASE 
            WHEN state = 'closed' THEN TO_CHAR(j.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')
        END AS closed_at,
        TO_CHAR(j.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(j.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        j.target_max_base_salary__c AS salary,
        j.seven20__total_value__c AS expected_fee,
        {{ build_location_locality
            ('NULL', 'NULL', 'seven20__city__c', 'NULL', 'NULL', 'seven20__country__c') 
        }} AS location_locality,
        j.seven20__city__c AS location_metro,
        j.seven20__country__c AS location_country,
        c.id AS company_id,
        c.atlas_id AS atlas_company_id,
        j.ownerid AS owner_id,
        u.atlas_id AS atlas_owner_id,
        '{{ var('agency_id')}}' AS agency_id
    FROM 
        {{ var('source_database') }}.seven20__job__c j
    INNER JOIN 
        {{ ref('4_companies_720') }} c ON c.id = j.seven20__account__c 
    LEFT JOIN 
         {{ var('source_database') }}.seven20__placement__c p ON p.seven20__job__c = j.id
    LEFT JOIN 
        {{ ref('1_users_720') }} u ON u.id = j.ownerid
    WHERE j.isdeleted = 0
),
source_leads AS (
    SELECT
        j.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || j.id") }} AS atlas_id,
        j.name AS job_role,
        regexp_replace(
            j.seven20__advert_description__c,
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS description,
        'full_time' contract_type,
        CASE 
            WHEN j.createddate < CURRENT_DATE - INTERVAL '6 months' THEN 'closed' 
            ELSE 'lead' 
        END AS state,
        CASE 
            WHEN state = 'closed' THEN 'lead_lost'
        END AS close_reason,
        CASE 
            WHEN state = 'closed' THEN TO_CHAR(j.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')
        END AS closed_at,
        TO_CHAR(j.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(j.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        NULL AS salary,
        NULL AS expected_fee,
        NULL AS location_locality,
        NULL AS location_metro,
        NULL AS location_country,
        c.id AS company_id,
        c.atlas_id AS atlas_company_id,
        j.ownerid AS owner_id,
        u.atlas_id AS atlas_owner_id,
        '{{ var('agency_id')}}' AS agency_id
    FROM 
        {{ var('source_database') }}.seven20__job_lead__c j
    INNER JOIN 
        {{ ref('4_companies_720') }} c ON c.id = j.seven20__account__c 
    LEFT JOIN 
        {{ ref('1_users_720') }} u ON u.id = j.ownerid
    WHERE j.isdeleted = 0
        AND j.seven20__job_lead_status__c != 'Converted'
)
SELECT
    id,
    atlas_id,
    job_role,
    description,
    contract_type,
    state,
    close_reason,
    closed_at,
    created_at,
    updated_at,
    salary,
    expected_fee,
    location_locality,
    location_metro,
    location_country,
    company_id,
    atlas_company_id,
    owner_id,
    COALESCE(atlas_owner_id, '{{ var("master_id") }}') AS atlas_owner_id,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at) as rn
    FROM (
        SELECT * FROM source_projects
        UNION ALL
        SELECT * FROM source_leads
    ) combined_data
) ranked_data
WHERE rn = 1