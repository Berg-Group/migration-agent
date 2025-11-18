{{ config(
    materialized='table',
    alias='educations_fr',
    tags=["seven20"]
) }}

with internal_persons AS (
SELECT
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM
    {{ref('people_fr')}}
)

SELECT 
    e.id,
    {{atlas_uuid('c.id || e.id')}} AS atlas_id,
	e.seven20__start_date__c::DATE as started_at,
	COALESCE(e.seven20__end_date__c, e.seven20__graduation_date__c)::DATE as finished_at,
	--e.seven20__degree_subject__c as field_of_study,
	e.seven20__school_name__c as name,
	'migration' as source,
	ip.person_id,
    ip.atlas_person_id
FROM 
	{{ var('source_database') }}."seven20__education_history__c" e
LEFT JOIN {{ var('source_database') }}."contact" c on c.id = e.seven20__candidate__c
LEFT JOIN internal_persons ip ON ip.person_id = c.id