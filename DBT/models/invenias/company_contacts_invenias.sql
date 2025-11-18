{{ config(
    materialized = 'table',
    alias = 'company_contacts_invenias',
    tags = ["invenias"]
) }}

WITH internal_persons AS (
  SELECT id AS person_id, atlas_id AS atlas_person_id
  FROM {{ ref('people_invenias') }}
),
internal_companies AS (
  SELECT id AS company_id, atlas_id AS atlas_company_id, relationship
  FROM {{ ref('companies_invenias') }}
),
person_company_positions AS (
  SELECT
    rp."personid" AS person_id,
    cp."companyid" AS company_id,
    rp."positionid" AS position_id,
    pcl."jobtitle" AS jobtitle,
    pcl."positionstatus" AS positionstatus,
    COALESCE(pcl."datemodified", pcl."datecreated", rp."datemodified", rp."datecreated") AS ts,
    ROW_NUMBER() OVER (
      PARTITION BY rp."personid", cp."companyid"
      ORDER BY COALESCE(pcl."datemodified", pcl."datecreated", rp."datemodified", rp."datecreated") DESC NULLS LAST
    ) AS rn
  FROM {{ var('source_database') }}."relation_persontoposition"   rp
  JOIN {{ var('source_database') }}."relation_companytoposition"  cp USING ("positionid")
  JOIN {{ var('source_database') }}."positions"                   pcl ON pcl."itemid" = rp."positionid"
  WHERE pcl."positionstatus" = 0
  AND pcl.enddate IS NULL 
),
last_title AS (
  SELECT person_id, company_id, jobtitle
  FROM person_company_positions
  WHERE rn = 1
)

SELECT
  p."itemid" AS person_id,
  ip.atlas_person_id AS atlas_person_id,
  {{ atlas_uuid('p.itemid || last_title.company_id') }} AS atlas_id,
  last_title.company_id AS company_id,
  ic.atlas_company_id AS atlas_company_id,
  TO_CHAR(p."datecreated"::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
  TO_CHAR(p."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
  CASE WHEN ic.relationship = 'client' THEN 'client' ELSE 'prospect' END AS relationship,
  COALESCE(last_title.jobtitle, 'contact') AS title
FROM {{ var('source_database') }}."people" p
JOIN last_title ON last_title.person_id = p."itemid"
INNER JOIN internal_persons ip ON ip.person_id = p."itemid"
INNER JOIN internal_companies ic ON ic.company_id = last_title.company_id