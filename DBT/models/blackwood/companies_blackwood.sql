{{ config(materialized='table', alias='companies_blackwood', tags=['blackwood']) }}

WITH projects AS (
    SELECT DISTINCT joblevel1companyid
    FROM {{ var('source_database') }}.jobfile
),

offlimits AS (
    SELECT
        CASE WHEN level2companyid IS NOT NULL THEN 'l2-'||level2companyid
             ELSE level1companyid::varchar END                    AS id,
        CASE offlimittype WHEN 'M' THEN 'soft' WHEN 'Y' THEN 'hard' END AS restriction_type,
        TO_CHAR(current_date,'YYYY-MM-DD"T"00:00:00')            AS restriction_created_at,
        expirydate                                               AS restriction_expiry_date,
        '{{ var("master_id") }}'                                 AS restriction_created_by_id,
        startfrom                                                AS restriction_note
    FROM {{ var('source_database') }}.offlimit
    WHERE expirydate >= current_date
),

first_save AS (
    SELECT  level2companyid,
            userid,
            TO_CHAR(savedatetime,'YYYY-MM-DD"T"00:00:00') AS first_dt,
            ROW_NUMBER() OVER (PARTITION BY level2companyid ORDER BY savedatetime) AS rn
    FROM {{ var('source_database') }}.level2company_savehistory
),

last_save AS (
    SELECT  level2companyid,
            userid,
            TO_CHAR(savedatetime,'YYYY-MM-DD"T"00:00:00') AS last_dt,
            ROW_NUMBER() OVER (PARTITION BY level2companyid ORDER BY savedatetime DESC) AS rn
    FROM {{ var('source_database') }}.level2company_savehistory
),

l1 AS (
    SELECT
        c.level1companyid::varchar                                    AS id,
        {{ atlas_uuid("'levelone'||c.level1companyid::text") }}       AS atlas_id,
        c.company_description                                         AS name,
        LOWER(TRIM(c.company_description))                            AS name_norm,
        NULL::varchar                                                 AS parent_name_norm,
        TO_CHAR(current_date,'YYYY-MM-DD"T"00:00:00')                 AS created_at,
        TO_CHAR(current_date,'YYYY-MM-DD"T"00:00:00')                 AS updated_at,
        '{{ var("master_id") }}'                                      AS created_by_atlas_id,
        '{{ var("master_id") }}'                                      AS updated_by_atlas_id,
        NULL::varchar                                                 AS location_locality,
        '{{ var("agency_id") }}'                                      AS agency_id,
        CASE WHEN p.joblevel1companyid IS NOT NULL THEN 'client' ELSE 'target' END AS relationship,
        NULL::varchar                                                 AS parent_company_id,
        1                                                             AS level
    FROM {{ var('source_database') }}.level1company c
    LEFT JOIN projects p ON p.joblevel1companyid = c.level1companyid
),

l2_raw AS (
    SELECT
        ('l2-'||l2.level2companyid)::varchar                           AS id,
        {{ atlas_uuid("'leveltwo'||l2.level2companyid::text") }}       AS atlas_id,
        l2.company_description                                         AS name,
        LOWER(TRIM(l2.company_description))                            AS name_norm,
        NULL::varchar                                                  AS parent_name_norm,
        COALESCE(f.first_dt,TO_CHAR(current_date,'YYYY-MM-DD"T"00:00:00')) AS created_at,
        COALESCE(ls.last_dt ,TO_CHAR(current_date,'YYYY-MM-DD"T"00:00:00')) AS updated_at,
        COALESCE(u1.atlas_id,'{{ var("master_id") }}')                 AS created_by_atlas_id,
        COALESCE(u2.atlas_id,'{{ var("master_id") }}')                 AS updated_by_atlas_id,
        loc.location_description::varchar                              AS location_locality,
        '{{ var("agency_id") }}'                                       AS agency_id,
        'target'                                                       AS relationship,
        l2.level1companyid::varchar                                    AS parent_company_id,
        2                                                              AS level
    FROM {{ var('source_database') }}.level2company l2
    LEFT JOIN first_save  f  ON f.level2companyid = l2.level2companyid AND f.rn = 1
    LEFT JOIN last_save   ls ON ls.level2companyid = l2.level2companyid AND ls.rn = 1
    LEFT JOIN {{ ref('users_blackwood') }} u1 ON u1.id = f.userid
    LEFT JOIN {{ ref('users_blackwood') }} u2 ON u2.id = ls.userid
    LEFT JOIN {{ var('source_database') }}.library_locations loc ON loc.location_code = l2.company_locationcode
),

parent_norm AS (
    SELECT id parent_id, name_norm parent_name_norm FROM l1
),

l2 AS (
    SELECT
        r.id,
        r.atlas_id,
        r.name,
        r.name_norm,
        COALESCE(p.parent_name_norm,r.parent_name_norm) AS parent_name_norm,
        r.created_at,
        r.updated_at,
        r.created_by_atlas_id,
        r.updated_by_atlas_id,
        r.location_locality,
        r.agency_id,
        r.relationship,
        r.parent_company_id,
        r.level
    FROM l2_raw r
    LEFT JOIN parent_norm p ON p.parent_id = r.parent_company_id
),

combined AS (
    SELECT id,atlas_id,name,name_norm,parent_name_norm,created_at,updated_at,
           created_by_atlas_id,updated_by_atlas_id,location_locality,agency_id,
           relationship,parent_company_id,level
    FROM l1
    UNION ALL
    SELECT id,atlas_id,name,name_norm,parent_name_norm,created_at,updated_at,
           created_by_atlas_id,updated_by_atlas_id,location_locality,agency_id,
           relationship,parent_company_id,level
    FROM l2
),

resolved AS (
    SELECT  
        COALESCE(m.new_company_id, c.id) AS new_id,
        c.atlas_id,
        c.name,
        c.created_at,
        c.updated_at,
        c.created_by_atlas_id,
        c.updated_by_atlas_id,
        c.location_locality,
        c.agency_id,
        c.relationship,
        c.parent_company_id,
        c.level
    FROM combined c
    LEFT JOIN {{ ref('companies_mapping_blackwood') }} m
           ON m.original_company_id = c.id
),

ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY new_id ORDER BY level, created_at) AS rn
    FROM resolved
)

SELECT
    new_id                               AS id,
    atlas_id,
    name,
    created_at,
    updated_at,
    created_by_atlas_id,
    updated_by_atlas_id,
    location_locality,
    agency_id,
    relationship,
    parent_company_id,
    level,
    o.restriction_type,
    o.restriction_created_at,
    o.restriction_expiry_date,
    o.restriction_created_by_id,
    o.restriction_note
FROM ranked
LEFT JOIN offlimits o ON o.id = new_id
WHERE rn = 1