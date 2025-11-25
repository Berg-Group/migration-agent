{{ config(
    materialized = 'table',
    alias = 'projects_invenias',
    tags=["invenias"]
) }}

WITH latest_status AS (
SELECT 
	ra.assignmentid AS project_id,
	MAX(ra.datecreated::date) as status_date

FROM 
	{{ var('source_database') }}."relation_assignmenttoprogresstracking" ra
GROUP BY 1
),

internal_companies AS (
SELECT 
    id AS company_id,
    atlas_id AS atlas_company_id 
FROM 
   {{ref('companies_invenias')}}
)

SELECT 
    a."itemid" AS id,
    {{atlas_uuid('a.itemid')}} AS atlas_id,
    "fileas" AS job_role,
    to_char(a."datecreated"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    to_char(a."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    'contract' AS contract_type,
    'project' AS class_type,
    '1' AS hire_targed,
    a."ownerid" AS owner_id,
    COALESCE(o.atlas_id, '{{var("master_id")}}') AS atlas_owner_id,
    false as public,
    CASE WHEN status.name IN ('Placement', 'Completed', 'Cancelled', 'Lost') 
            THEN 'closed' 
         WHEN status.name = 'On Hold' THEN 'on_hold'
         WHEN status.name IN ('Active', 'Offer') THEN 'active'
         WHEN status.name IN ('Speculative', 'Pending') THEN 'lead' 
          WHEN status.name ISNULL THEN 'on_hold' END AS state,
    CASE WHEN status.name IN ('Placement', 'Completed', 'Cancelled', 'Lost') 
            THEN to_char(latest_status.status_date::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS') ELSE NULL END AS closed_at,
    CASE WHEN status.name IN ('Placement', 'Completed') 
            THEN 'won' 
        WHEN status.name IN ('Cancelled', 'Lost') 
            THEN 'worked_lost' END AS close_reason,
    '{{ var('agency_id')}}' AS agency_id,
    ic.company_id,
    ic.atlas_company_id,
    a.assignmentnumber AS job_number
FROM
    {{ var('source_database') }}."assignments" a 
LEFT JOIN {{ var('source_database') }}."relation_companytoassignment" ca ON ca.assignmentid = a.itemid  
INNER JOIN internal_companies ic ON ic.company_id = ca.companyid
LEFT JOIN 
    {{ref('users_invenias')}} o ON o.id = a.ownerid
LEFT JOIN 
    {{ var('source_database') }}."lookuplistentries" status ON status.itemid = a.status
LEFT JOIN 
    latest_status ON latest_status.project_id = a."itemid"
