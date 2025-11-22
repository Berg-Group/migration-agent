{{ config(
    materialized='table',
    alias='project_users_invenias',
    tags=["invenias"]
) }}

WITH internal_projects AS (
  SELECT  id AS project_id, 
        atlas_id AS atlas_project_id
  FROM {{ ref('projects_invenias') }}
),
internal_users AS (
  SELECT id AS user_id, 
        atlas_id AS atlas_user_id
  FROM {{ ref('users_invenias') }}
),
team_users AS (
  SELECT
    a2tm.assignmentid AS project_id,
    utp.userid        AS user_id
  FROM {{ var('source_database') }}."relation_assignmenttoteammember" a2tm
  INNER JOIN {{ var('source_database') }}."relation_persontoteammember" ptm
    ON ptm.teammemberid = a2tm.teammemberid
  INNER JOIN {{ var('source_database') }}."relation_usertoperson" utp
    ON utp.personid = ptm.personid
),
owner_users AS (
  SELECT
    a2o.assignmentid AS project_id,
    a2o.userid       AS user_id
  FROM {{ var('source_database') }}."relation_assignmenttoowner" a2o
),
unified AS (
  SELECT project_id, user_id FROM team_users
  UNION
  SELECT project_id, user_id FROM owner_users
)

SELECT 
    {{atlas_uuid('ip.project_id || iu.user_id')}} AS atlas_id,
  to_char(current_timestamp, 'YYYY-MM-DD"T00:00:00') AS created_at,
  to_char(current_timestamp, 'YYYY-MM-DD"T00:00:00') AS updated_at,
  '{{ var("agency_id") }}' AS agency_id,
  u.project_id,
  ip.atlas_project_id,
  iu.user_id,
  iu.atlas_user_id
FROM unified u
INNER JOIN internal_projects ip USING (project_id)
INNER JOIN internal_users   iu USING (user_id)
