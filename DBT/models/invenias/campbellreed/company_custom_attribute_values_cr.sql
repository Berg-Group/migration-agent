{{ config(
    materialized='table',
    alias='company_custom_attribute_values_cr'
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ref('companies_invenias')}}
),
internal_options AS (
    SELECT 
        atlas_attribute_id,
        atlas_id AS option_id,
        id AS external_id
    FROM 
        {{ref('custom_attribute_options_cr')}}

),

engagement_raw AS (
  SELECT
    rca.companyid                        AS company_id,
    a.itemid                             AS assignment_id,
    le.itemid                            AS engagement_type_id,
    le.name                              AS engagement_type,
    COALESCE(a.datemodified, a.datecreated) AS ts,
    ROW_NUMBER() OVER (
      PARTITION BY rca.companyid
      ORDER BY COALESCE(a.datemodified, a.datecreated) DESC
    ) AS rn
  FROM {{ var('source_database') }}."relation_companytoassignment" rca
  JOIN {{ var('source_database') }}."assignments" a
    ON a.itemid = rca.assignmentid
  JOIN {{ var('source_database') }}."lookuplistentries" le
    ON le.itemid = a.engagementtype
  JOIN {{ var('source_database') }}."lookuplists" ll
    ON ll.itemid = le.lookuplistid
   AND ll.name = 'EngagementType'
),
engagement AS (
  SELECT company_id, engagement_type_id, engagement_type, ts
  FROM engagement_raw
  WHERE rn = 1
)

SELECT
    {{ atlas_uuid('e.company_id || io.option_id') }} AS atlas_id,
    ic.company_id,
    ic.atlas_company_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(current_timestamp::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(current_timestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
FROM 
     engagement e
INNER JOIN 
    internal_companies ic USING (company_id)
INNER JOIN 
    internal_options io ON io.external_id = e.engagement_type_id