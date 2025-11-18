{{ config(
    materialized='table',
    alias='people_720',
    tags=["seven20"]
) }}

SELECT 
	c.id AS id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.id::text") }} AS atlas_id,
	c.firstname AS first_name,
	c.lastname AS last_name,
    TO_CHAR(c.createddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(c.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    'active' AS responsiveness,
    'regular' AS candidate_status,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingstreet,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_street_address,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingcity,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_metro,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingstate,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingpostalcode,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_postal_code,
    BTRIM(REGEXP_REPLACE(COALESCE(c.mailingcountry,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
    {{ build_location_locality
        ('c.mailingstreet', 'NULL', 'c.mailingcity', 'c.mailingstate', 'c.mailingpostalcode', 'c.mailingcountry')
    }} AS location_locality,
    TRIM(
        CASE 
            WHEN c.experience__c IS NOT NULL AND c.experience__c != '' 
            THEN 'Experience: ' || CHR(13) || CHR(10) || c.experience__c || CHR(13) || CHR(10) || CHR(13) || CHR(10)
            ELSE '' 
        END ||
        CASE 
            WHEN c.motivations__c IS NOT NULL AND c.motivations__c != '' 
            THEN 'Motivations: ' || CHR(13) || CHR(10) || c.motivations__c || CHR(13) || CHR(10) || CHR(13) || CHR(10)
            ELSE '' 
        END ||
        CASE 
            WHEN c.needs__c IS NOT NULL AND c.needs__c != '' 
            THEN 'Needs: ' || CHR(13) || CHR(10) || c.needs__c || CHR(13) || CHR(10) || CHR(13) || CHR(10)
            ELSE '' 
        END ||
        CASE 
            WHEN c.alternative_activity__c IS NOT NULL AND c.alternative_activity__c != '' 
            THEN 'Alternative Activity: ' || CHR(13) || CHR(10) || c.alternative_activity__c || CHR(13) || CHR(10) || CHR(13) || CHR(10)
            ELSE '' 
        END ||
        CASE 
            WHEN c.sell__c IS NOT NULL AND c.sell__c != '' 
            THEN 'Sell: ' || CHR(13) || CHR(10) || c.sell__c
            ELSE '' 
        END
    ) AS overview,
    c.createdbyid AS created_by_id,
    c.lastmodifiedbyid AS updated_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
    '{{ var('agency_id') }}' AS agency_id
FROM 
	{{ var('source_database') }}.contact c 
LEFT JOIN 
    "{{ this.schema }}".users_720 u ON u.id = c.createdbyid
LEFT JOIN 
    "{{ this.schema }}".users_720 u2 ON u2.id = c.lastmodifiedbyid
WHERE 
    c.isdeleted = 0 AND c.id NOT IN (SELECT contact_id FROM {{ ref('people_dupes_720') }}) 