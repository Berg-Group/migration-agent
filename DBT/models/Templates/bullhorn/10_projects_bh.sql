{{ config(
    materialized = 'table',
    alias        = 'projects_bh'
) }}

WITH jobop AS (
    SELECT
        jo.JobPostingID,
        jo.linkedjobpostingid,
        jo.Title,
        jo.EmploymentType,
        jo.Salary,
        jo.PublicDescription,
        jo.Description,
        jo.UserID,
        jo.ClientCorporationID,
        jo.Status,
        jo.DateAdded,
        jo.DateClosed,
        jo.address,
        jo.address2,
        jo.city,
        jo.state AS region,
        jo.zip,
        jo.isopportunity,
        jo.isopen
    FROM {{ var('source_database') }}."bh_jobopportunity" jo
    WHERE jo.isdeleted = 0
),
excluded_ids AS (
    SELECT DISTINCT
        linkedjobpostingid AS exclude_me
    FROM jobop
    WHERE linkedjobpostingid IS NOT NULL
),
filtered_jobop AS (
    SELECT
        jo.*
    FROM jobop jo
    WHERE jo.JobPostingID NOT IN (SELECT exclude_me FROM excluded_ids)
),
projects AS (
    SELECT
        fj.JobPostingID AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || fj.JobPostingID::text") }} AS atlas_id,
        TO_CHAR(fj.DateAdded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(fj.DateAdded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        TO_CHAR(fj.DateClosed::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS closed_at,
        fj.Title AS job_role,
        CASE
            WHEN fj.EmploymentType IN ('Permanent', 'RFP', 'Ovyo Opportunity', 'Opportunity', 'Full-time', 
                'Speculative', 'BD Opportunity', 'AD Opportunity', 'Retained Search', 'Retained') THEN 'full_time'
            WHEN fj.EmploymentType IN ('Contract', 'Temporary', 'Fixed Term', 'RAAS') THEN 'contract'
            ELSE fj.EmploymentType
        END AS contract_type,
        fj.Salary AS salary,
        regexp_replace(fj.PublicDescription, '<[^>]*>', '') AS job_description,
        regexp_replace(fj.Description, '<[^>]*>', '') AS notes,
        fj.UserID AS owner_id,
        fj.ClientCorporationID AS company_id,
        fj.Status AS previous_status,
        fj.DateAdded AS dateadded_raw,
        fj.DateClosed AS dateclosed_raw,
        CASE 
            WHEN fj.linkedjobpostingid IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS external_assignment,
        fj.address,
        fj.address2,
        fj.city,
        fj.region,
        fj.zip,
        fj.isopportunity,
        fj.isopen
    FROM filtered_jobop fj
),
projects_with_company AS (
    SELECT
        p.*,
        c.atlas_id AS atlas_company_id
    FROM projects p
    LEFT JOIN {{ ref('3_companies_bh') }} c ON p.company_id = c.id
),
projects_with_users AS (
    SELECT
        pwc.*,
        u.atlas_id AS atlas_owner_id
    FROM projects_with_company pwc
    LEFT JOIN {{ ref('0_users_bh') }} u ON pwc.owner_id = u.id
),
final_projects AS (
    SELECT
        pwu.*,
        CASE
            WHEN LOWER(TRIM(pwu.previous_status)) IN ('qualifying') THEN 'lead'
            WHEN LOWER(TRIM(pwu.previous_status)) IN (
                '1 qualified', '2 resourcing', '3 interviews', '4 final interview', '5 offer out', 'accepting candidates', 'open', 'converted', 'offer out',
                'negotiating'
            ) THEN 'active'
            WHEN LOWER(TRIM(pwu.previous_status)) IN (
                '7 on hold / cancelled', '8 - filled internally', '8 filled internally', '9 filled by competitor', 
                'archive', 'cancelled', 'client replied', 'closed', 'closed-lost', 'closed-won', 'contacted', 'contacted client',
                'filled by client', 'lost to competitor', 'placed', 'replied', 'spec cv sent', '6 placed', '10 cancelled'
            ) THEN 'closed'
            WHEN LOWER(TRIM(pwu.previous_status)) IN (
                '7 - on hold', '7 on hold', 'on hold'
            ) THEN 'on_hold'
            ELSE 'closed'
        END AS state,
        CASE
            WHEN state = 'closed' THEN
                CASE
                    WHEN LOWER(TRIM(pwu.previous_status)) IN ('10 cancelled', '7 on hold / cancelled', 'archive', 'closed', 'cancelled') THEN 'cancelled'
                    WHEN LOWER(TRIM(pwu.previous_status)) IN ('6 placed', 'placed', 'closed-won') THEN 'won'
                    WHEN LOWER(TRIM(pwu.previous_status)) IN ('spec cv sent') THEN 'lead_lost'
                    ELSE 'worked_lost'
                END
            ELSE NULL
        END AS close_reason,
        CASE
            WHEN state = 'closed' THEN 
                CASE
                    WHEN pwu.dateclosed_raw IS NOT NULL THEN pwu.closed_at
                    ELSE pwu.updated_at
                END
            ELSE NULL
        END AS final_closed_at
    FROM projects_with_users pwu
)
SELECT
    atlas_id,
    id,
    created_at,
    updated_at,
    final_closed_at AS closed_at,
    job_role,
    contract_type,
    salary,
    job_description,
    notes,
    owner_id,
    COALESCE(atlas_owner_id, '{{ var("master_id") }}') AS atlas_owner_id,
    company_id,
    atlas_company_id,
    previous_status,
    state,
    close_reason,
    external_assignment,
    CASE 
        WHEN TRIM(address) ~* '[a-zA-Z].*[a-zA-Z]' OR 
             TRIM(address2) ~* '[a-zA-Z].*[a-zA-Z]' OR
             TRIM(city) ~* '[a-zA-Z].*[a-zA-Z]' OR
             TRIM(region) ~* '[a-zA-Z].*[a-zA-Z]' OR
             TRIM(zip) ~ '^[0-9]{3,}$'
        THEN TRIM(
            CASE WHEN address IS NOT NULL THEN TRIM(address) ELSE '' END ||
            CASE WHEN address2 IS NOT NULL THEN ', ' || TRIM(address2) ELSE '' END ||
            CASE WHEN city IS NOT NULL THEN ', ' || TRIM(city) ELSE '' END ||
            CASE WHEN region IS NOT NULL THEN ', ' || TRIM(region) ELSE '' END ||
            CASE WHEN zip IS NOT NULL THEN ', ' || TRIM(zip) ELSE '' END
        )
    END AS location_locality,
    CASE 
        WHEN TRIM(address) ~* '[a-zA-Z].*[a-zA-Z]' THEN TRIM(address) 
    END AS location_street_address,
    CASE 
        WHEN TRIM(city) ~* '[a-zA-Z].*[a-zA-Z]' THEN TRIM(city) 
    END AS location_metro,
    CASE 
        WHEN TRIM(region) ~* '[a-zA-Z].*[a-zA-Z]' THEN TRIM(region) 
    END AS location_region,
    CASE 
        WHEN TRIM(zip) ~ '^[0-9]{3,}$' THEN TRIM(zip) 
    END AS location_postal_code,
    id AS job_number
FROM final_projects
-- NOTE: This WHERE clause is specific to an agency and needs confirmation on every migration
-- WHERE ((isopportunity = false) OR (isopportunity = true AND isopen = 1))