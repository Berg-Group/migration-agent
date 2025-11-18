{{ config(
    materialized = 'table',
    alias        = 'merged_map_fr',
    tags         = ['seven20']
) }}

SELECT 
	c.id AS merge_person_id, 
    c.seven20__related_candidate__c AS person_id
FROM 
    {{ var('source_database') }}.contact c 
WHERE 
    c.seven20__related_candidate__c IS NOT NULL AND c.isdeleted = 0

UNION ALL

SELECT 
	c.id AS merge_person_id, 
    c.seven20__related_client_contact__c AS person_id
FROM 
    {{ var('source_database') }}.contact c 
WHERE 
    c.seven20__related_client_contact__c IS NOT NULL AND c.isdeleted = 0