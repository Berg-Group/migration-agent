{{ config(
    materialized='table',
    alias='project_users_blackwood',
    tags=["blackwood"]
) }}

WITH internal_projects AS (
SELECT 
    DISTINCT id,
    atlas_id 
FROM
    {{ref('projects_blackwood')}}
)

SELECT 
    lower(
            substring(md5(ipr.id::text || u.atlas_id), 1, 8) || '-' ||
            substring(md5(ipr.id::text || u.atlas_id), 9, 4) || '-' ||
            substring(md5(ipr.id::text || u.atlas_id), 13, 4) || '-' ||
            substring(md5(ipr.id::text || u.atlas_id), 17, 4) || '-' ||
            substring(md5(ipr.id::text || u.atlas_id), 21, 12)) AS atlas_id,    
    to_char(current_timestamp, 'YYYY-MM-DD"T00:00:00') AS created_at,
    to_char(current_timestamp, 'YYYY-MM-DD"T00:00:00') AS updated_at,
    '{{var('agency_id')}}' AS agency_id,
    j.jobid AS project_id, 
    ipr.atlas_id AS atlas_project_id,
    u.id AS  user_id,
    u.atlas_id AS atlas_user_id
FROM 
    {{ var('source_database') }}."jobfile" AS j 
LEFT JOIN 
    internal_projects ipr ON ipr.id = j.jobid
LEFT JOIN {{ var('source_database') }}."jobfile_leadconsultant" jl USING (jobid)
LEFT JOIN 
    {{ ref('users_blackwood') }} u ON u.id = jl.userid
WHERE   
    u.atlas_id NOTNULL