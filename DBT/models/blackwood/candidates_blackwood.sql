{{ config(materialized='table', alias='candidates_blackwood', tags=['blackwood']) }}

WITH internal_persons AS (
    SELECT id AS person_id,
           atlas_id AS atlas_person_id
    FROM {{ ref('people_blackwood') }}
),

internal_projects AS (
    SELECT id AS project_id,
           atlas_id AS atlas_project_id,
           owner_id,
           atlas_owner_id
    FROM {{ ref('projects_blackwood') }}
),

contact_logs AS (
    SELECT
        cc.contactlog_id,
        cc.cref,
        cc.jobid,
        cc.contact_purpose,
        cc.contact_action,
        cc.contact_date,
        cc.contact_mode,
        COALESCE(lcp.purpose_description, cc.contact_purpose::TEXT, '') AS purpose_description,
        ca.action_description
    FROM {{ var('source_database') }}.candidate_contactlog cc
    LEFT JOIN {{ var('source_database') }}.library_contactlog_purpose lcp  ON lcp.purpose_code = cc.contact_purpose
    LEFT JOIN {{ var('source_database') }}.library_contactlog_actions ca ON ca.action_code = cc.contact_action
    WHERE cc.jobid IS NOT NULL
),

norm AS (
    SELECT DISTINCT
        *,
        LOWER(TRIM(purpose_description)) AS desc_norm,
        LOWER(TRIM(contact_purpose))     AS code_norm,
        LOWER(TRIM(action_description))  AS act_norm
    FROM contact_logs
),

comments_raw AS (
    SELECT 
        cref  AS person_id,
        jobid AS project_id,
        'internal notes' || '\n\n' || contact_internalnotes || '\n\n' AS comment
    FROM {{ var('source_database') }}.candidate_contactlog
    WHERE contact_internalnotes IS NOT NULL
      AND TRIM(contact_internalnotes) <> ''
    UNION ALL
    SELECT 
        cref,
        jobid,
        'admin action' || '\n\n' || contact_adminaction AS comment
    FROM {{ var('source_database') }}.candidate_contactlog
    WHERE contact_adminaction IS NOT NULL
      AND TRIM(contact_adminaction) <> ''
),

comments AS (
    SELECT 
        person_id,
        project_id,
        listagg(comment, '\n\n') within group (order by comment) as comment
    FROM comments_raw
    GROUP BY person_id, project_id
),

q AS (
    SELECT
        cl.contactlog_id                                                           AS id,
        {{atlas_uuid('cl.cref || cl.jobid')}}                                      AS atlas_id,
        cl.cref                                                                    AS person_id,
        ip.atlas_person_id,
        cl.jobid                                                                   AS project_id,
        ipp.atlas_project_id,
        'Candidate'                                                                AS class_type,
        CASE
            WHEN cl.contact_action = '$' THEN 'Hired'
            WHEN cl.contact_action IN ('R','IH') THEN 'Ringfence'
            WHEN (cl.code_norm = 'shortlist' OR cl.desc_norm LIKE '%shortlist%') AND cl.contact_action IN ('A','P1','P2','P3') THEN 'In process'
            WHEN (cl.code_norm = 'shortlist' OR cl.desc_norm LIKE '%shortlist%') AND cl.contact_action = 'H' THEN 'On Hold'
            WHEN (cl.code_norm = 'shortlist' OR cl.desc_norm LIKE '%shortlist%') AND cl.contact_action = 'Xcl' THEN 'Client Declined'
            WHEN (cl.code_norm = 'shortlist' OR cl.desc_norm LIKE '%shortlist%') AND cl.contact_action = 'Xca' THEN 'Candidate Declined'
            WHEN (cl.code_norm = 'shortlist' OR cl.desc_norm LIKE '%shortlist%') AND cl.contact_action = 'Clo' THEN 'Client closed off talks'
            WHEN (cl.code_norm != 'shortlist' AND cl.desc_norm NOT LIKE '%shortlist%') AND cl.contact_action = 'Xcl' THEN 'Client declined based on profile'
            WHEN (cl.code_norm != 'shortlist' AND cl.desc_norm NOT LIKE '%shortlist%') AND cl.contact_action = 'Xca' THEN 'Candidate declined after approach'
            WHEN (cl.desc_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu') OR cl.code_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu')) AND cl.contact_action = 'Xbg' AND (cl.contact_mode IS NULL OR cl.contact_mode = '' OR cl.contact_mode NOT IN ('T','Z','M')) THEN 'We declined on profile'
            WHEN (cl.code_norm != 'shortlist' AND cl.desc_norm NOT LIKE '%shortlist%') AND cl.contact_action = 'Xbg' AND cl.contact_mode IN ('T','Z','M') THEN 'Blackwood declined'
            WHEN (cl.desc_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu') OR cl.code_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu')) AND cl.contact_mode IN ('Z','M') AND cl.contact_action NOT IN ('Xbg','Xca','Xcl') THEN 'Blackwood Met'
            WHEN (cl.desc_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu') OR cl.code_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu')) AND cl.contact_mode = 'T' AND cl.contact_action NOT IN ('Xbg','Xca','Xcl') THEN 'Candidate considering'
            WHEN (cl.desc_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu') OR cl.code_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu')) AND cl.contact_mode IN ('E','L','O','S') THEN 'Reached Out'
            WHEN (cl.desc_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu') OR cl.code_norm IN ('update','map','incontact','invite','calibrating','meeting','candconsid','prospect','ccu')) AND (cl.contact_mode IS NULL OR cl.contact_mode = '') THEN 'For consideration'
            WHEN (cl.code_norm = 'source' OR cl.desc_norm LIKE 'source%' OR cl.code_norm = 'ref' OR cl.desc_norm LIKE 'ref%') AND cl.contact_mode IN ('T','Z','M') THEN 'Sourced'
            WHEN (cl.code_norm = 'source' OR cl.desc_norm LIKE 'source%' OR cl.code_norm = 'ref' OR cl.desc_norm LIKE 'ref%') AND (cl.contact_mode IS NULL OR cl.contact_mode = '' OR cl.contact_mode IN ('E','L','O','S')) THEN 'To source'
            ELSE 'To source'
        END                                                                          AS status,
        CASE
            WHEN cl.action_description = 'Candidate closed off talks' THEN 'self'
            WHEN cl.action_description = 'Blackwood closed off talks' THEN 'by_us'
            WHEN cl.action_description = 'Client closed off talks'    THEN 'by_client'
        END                                                                          AS rejection_type,
        CASE
            WHEN cl.action_description IN ('Client closed off talks','Candidate closed off talks','Blackwood closed off talks') THEN 'other'
        END                                                                          AS rejection_reason,
        CASE
            WHEN cl.action_description IN ('Client closed off talks','Candidate closed off talks','Blackwood closed off talks') THEN TO_CHAR(cl.contact_date,'YYYY-MM-DD"T"00:00:00')
        END                                                                          AS rejected_at,
        CASE
            WHEN cl.action_description IN ('Client closed off talks','Candidate closed off talks','Blackwood closed off talks') THEN COALESCE(ipp.atlas_owner_id,'{{ var("master_id") }}')
        END                                                                          AS rejected_by_atlas_id,
        ipp.owner_id,
        COALESCE(ipp.atlas_owner_id,'{{ var("master_id") }}')                         AS atlas_owner_id,
        c.comment,
        cl.contact_date
    FROM norm cl
    left join internal_persons ip on ip.person_id = cl.cref
    left join internal_projects ipp on ipp.project_id = cl.jobid
    left join comments c using (person_id, project_id)
),

dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY person_id, project_id ORDER BY contact_date DESC) AS rn
    FROM q)

SELECT * FROM dedup WHERE rn = 1