{{ config(
    materialized = 'table',
    alias        = 'people_dupes_720',
    tags         = ['seven20']
) }}

SELECT 
	c.id AS contact_id, 
    c.seven20__related_candidate__c AS candidate_id
FROM 
    {{ var('source_database') }}.contact c 
WHERE 
    c.seven20__related_candidate__c IS NOT NULL