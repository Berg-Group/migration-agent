{{ config(
    materialized='table',
    alias='people_720',
    tags=["seven20"]
) }}


SELECT 
	c.id as id,
    LOWER(
        SUBSTRING(MD5(c.id::text), 1, 8) || '-' ||
        SUBSTRING(MD5(c.id::text), 9, 4) || '-' ||
        SUBSTRING(MD5(c.id::text), 13, 4) || '-' ||
        SUBSTRING(MD5(c.id::text), 17, 4) || '-' ||
        SUBSTRING(MD5(c.id::text), 21, 12)
    ) AS atlas_id,
	c.firstname as first_name,
	c.lastname as last_name,
    to_char(c.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    to_char(c.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS updated_at,
	c.createdbyid as created_by,
    u.atlas_id AS created_by_atlas_id,
	--coalesce(r.name, c.seven20__record_type_name__c) as record_type,
    '{{ var('agency_id') }}' AS agency_id,
    'active' AS responsiveness,
    'regular' AS candidate_status,
    c.mailingcity AS location_locality
FROM 
	{{ var('source_database') }}."contact" as c 
LEFT JOIN {{ var('source_database') }}.account a on a.id = c."accountId"
--LEFT JOIN {{ var('source_database') }}.recordtype r on r.id = c.seven20__record_type_name__c
LEFT JOIN "{{ this.schema }}"."users" u ON u.id = c.createdbyid
WHERE c.isdeleted = FALSE 