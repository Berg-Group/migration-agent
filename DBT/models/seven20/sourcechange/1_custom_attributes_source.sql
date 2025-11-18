{{ config(
    materialized='table',
    alias='custom_attributes_source',
    tags = ["seven20"]
) }}

WITH custom_attributes AS (
    SELECT 'Employer Type' AS entity_name
    UNION ALL
    SELECT 'Seniority' AS entity_name
    UNION ALL
    SELECT 'Candidate Source' AS entity_name
    UNION ALL
    SELECT 'Candidate Source Detail' AS entity_name
    UNION ALL
    SELECT 'Preferred Work Location' AS entity_name
    UNION ALL
    SELECT 'Open to Relocation?' AS entity_name
    UNION ALL
    SELECT 'Project Employer Type' AS entity_name
    UNION ALL
    SELECT 'Project Employment Type' AS entity_name
    UNION ALL
    SELECT 'Project Seniority' AS entity_name
    UNION ALL
    SELECT 'Company Employer Type' AS entity_name
    UNION ALL
    SELECT 'Function' AS entity_name
    UNION ALL
    SELECT 'Coverage' AS entity_name
    UNION ALL
    SELECT 'Project Function' AS entity_name
    UNION ALL
    SELECT 'Project Coverage' AS entity_name
    UNION ALL
    SELECT 'Company Function' AS entity_name
    UNION ALL
    SELECT 'Company Coverage' AS entity_name
    UNION ALL
    SELECT 'Candidate' AS entity_name
    UNION ALL
    SELECT 'Client' AS entity_name
)
SELECT
    {{ atlas_uuid("'custom' || entity_name || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Employer Type' THEN 'employer_type'
        WHEN entity_name = 'Seniority' THEN 'seniority'
        WHEN entity_name = 'Candidate Source' THEN 'candidate_source'
        WHEN entity_name = 'Candidate Source Detail' THEN 'candidate_source_detail'
        WHEN entity_name = 'Preferred Work Location' THEN 'preferred_work_location'
        WHEN entity_name = 'Open to Relocation?' THEN 'open_to_relocation'
        WHEN entity_name = 'Project Employer Type' THEN 'project_employer_type'
        WHEN entity_name = 'Project Employment Type' THEN 'project_employment_type'
        WHEN entity_name = 'Project Seniority' THEN 'project_seniority'
        WHEN entity_name = 'Company Employer Type' THEN 'company_employer_type'
        WHEN entity_name = 'Function' THEN 'function'
        WHEN entity_name = 'Coverage' THEN 'coverage'
        WHEN entity_name = 'Project Function' THEN 'project_function'
        WHEN entity_name = 'Project Coverage' THEN 'project_coverage'
        WHEN entity_name = 'Company Function' THEN 'company_function'
        WHEN entity_name = 'Company Coverage' THEN 'company_coverage'
        WHEN entity_name = 'Candidate' THEN 'candidate'
        WHEN entity_name = 'Client' THEN 'client'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    CASE
        WHEN entity_name IN ('Employer Type', 'Preferred Work Location') THEN TRUE
        ELSE FALSE
    END AS multiple_values,
    FALSE AS ai,
    'options' AS type,
    CASE 
        WHEN entity_name IN (
            'Employer Type', 'Seniority', 'Candidate Source', 'Candidate Source Detail', 'Preferred Work Location', 'Open to Relocation?', 'Function', 'Coverage'
        ) THEN 'person'
        WHEN entity_name IN ('Project Employer Type', 'Project Employment Type', 'Project Seniority', 'Project Function', 'Project Coverage') THEN 'project'
        WHEN entity_name IN ('Company Employer Type', 'Company Function', 'Company Coverage') THEN 'company'
        WHEN entity_name IN ('Candidate', 'Client') THEN 'interview'
        ELSE 'person'
    END AS of
FROM custom_attributes