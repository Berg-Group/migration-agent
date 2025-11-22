{{ config(
    materialized='table',
    alias='projects_ff',
    tags=["filefinder"]
) }}

WITH base_projects AS (
SELECT 
    a.idassignment AS id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || a.idassignment::text") }} AS atlas_id,
    cf.id AS company_id,
    cf.atlas_id AS atlas_company_id,
    TO_CHAR(a.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
    TO_CHAR(a.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
    a.assignmenttitle AS job_role,
    {{ clean_html('a.AssignmentBrief') }} AS description,
    {{ clean_html('a.assignmentcomment') }} AS notes,
    CASE 
        WHEN LOWER(t.value) IN ('single','multiple','ned') THEN 'full_time'
        WHEN LOWER(t.value) LIKE '%contract%' OR LOWER(t.value) LIKE '%interim%' OR LOWER(t.value) LIKE '%temp%' THEN 'contract'
        ELSE 'full_time'
    END AS contract_type,
    CASE 
        WHEN a.createdon::date < DATEADD(month, -6, CURRENT_DATE) THEN 'closed'
        WHEN LOWER(s.value) IN (
            'assign complete - placed by us', 'assign complete - cancelled', 'rejected lob-in', 'never start',
            'assign complete - no placement', 'assign complete - internal placement', 'pitch complete - lost',
            'pitch complete - cancelled', 'finished'
        ) THEN 'closed'
        WHEN LOWER(s.value) = 'assign - on hold' THEN 'on_hold'
        WHEN LOWER(s.value) = 'assign - active' THEN 'active'
        WHEN LOWER(s.value) IN ('prospect','pitch - active') THEN 'lead'
        ELSE 'active'
    END AS state,
    CASE 
        WHEN LOWER(s.value) IN ('assign complete - placed by us','finished','assign complete - no placement','assign complete - internal placement') THEN 'won'
        WHEN LOWER(s.value) IN ('assign complete - cancelled','pitch complete - cancelled','never start') THEN 'cancelled'
        WHEN LOWER(s.value) IN ('rejected lob-in') THEN 'worked_lost'
        WHEN LOWER(s.value) IN ('pitch complete - lost') THEN 'lead_lost'
        WHEN state = 'closed' THEN 'worked_lost'
        ELSE NULL
    END AS close_reason,
    CASE
        WHEN state = 'closed' THEN TO_CHAR(COALESCE(a.actualcompletedate, CURRENT_TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ELSE NULL
    END AS closed_at,
    CASE 
        WHEN COALESCE(a.salaryfrom, 0) = 0 AND COALESCE(a.salaryto, 0) = 0 THEN NULL
        WHEN a.salaryfrom IS NOT NULL AND a.salaryto IS NOT NULL THEN 
            CASE 
                WHEN a.salaryfrom = 0 THEN TRIM(a.salaryto::text)
                WHEN a.salaryto = 0 THEN TRIM(a.salaryfrom::text)
                ELSE TRIM(a.salaryfrom::text) || ' - ' || TRIM(a.salaryto::text)
            END
        WHEN a.salaryfrom IS NOT NULL AND a.salaryfrom <> 0 THEN TRIM(a.salaryfrom::text)
        WHEN a.salaryto IS NOT NULL AND a.salaryto <> 0 THEN TRIM(a.salaryto::text)
        ELSE NULL
    END AS salary,
    c2.value AS salary_currency,
    uf.id AS owner_id,
    COALESCE(uf.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    a.assignmentno AS job_number,
    ROW_NUMBER() OVER (
        PARTITION BY a.assignmentno
        ORDER BY a.createdon ASC, a.idassignment ASC
    ) AS job_number_rank,
    'project' AS class_type,
    '{{ var('agency_id')}}' AS agency_id
FROM {{ var('source_database') }}."assignment" a 
INNER JOIN {{ this.schema }}.companies_ff cf ON cf.id = a.idcompany 
LEFT JOIN {{ this.schema }}.users_ff uf ON uf.id = a.iduser
LEFT JOIN {{ var('source_database') }}.assignmentstatus s ON s.idassignmentstatus = a.idassignmentstatus 
LEFT JOIN {{ var('source_database') }}.assignmenttype t ON t.idassignmenttype = a.idassignmenttype 
LEFT JOIN {{ var('source_database') }}.currency c2 ON c2.idcurrency = a.idcurrency1
WHERE LOWER(s.value) NOT IN ('research/reference') AND a.IsDeleted != 1
),
final_projects AS (
    SELECT 
        b.id,
        b.atlas_id,
        b.company_id,
        b.atlas_company_id,
        b.created_at,
        b.updated_at,
        b.job_role,
        b.description,
        b.notes,
        b.contract_type,
        b.state,
        b.close_reason,
        b.closed_at,
        b.salary,
        b.salary_currency,
        b.owner_id,
        b.atlas_owner_id,
        CASE WHEN b.job_number_rank = 1 THEN b.job_number ELSE NULL END AS job_number,
        b.class_type,
        b.agency_id,
        ROW_NUMBER() OVER (
            PARTITION BY b.id
            ORDER BY b.created_at ASC, b.id ASC
        ) AS rn
    FROM base_projects b
)
SELECT 
    id,
    atlas_id,
    company_id,
    atlas_company_id,
    created_at,
    updated_at,
    job_role,
    description,
    notes,
    contract_type,
    state,
    close_reason,
    closed_at,
    salary,
    salary_currency,
    owner_id,
    atlas_owner_id,
    job_number,
    class_type,
    agency_id
FROM final_projects
WHERE rn = 1

