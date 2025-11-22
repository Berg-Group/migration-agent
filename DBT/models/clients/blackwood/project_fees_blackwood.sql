{{ config(
    materialized='table',
    alias='project_fees_blackwood',
    tags=["blackwood"]
) }}

with internal_fees AS (
SELECT 
    atlas_id,
    name
FROM 
    {{ ref('fee_types_blackwood') }}
),

internal_projects AS (
SELECT 
    id,
    atlas_id 
FROM 
    {{ ref('projects_blackwood') }}
),

internal_users AS (
SELECT 
    id,
    atlas_id
FROM 
    {{ ref('users_blackwood') }}
),

latest_transaction AS (
SELECT 
    retainerid,
    valueamount,
    jobid,
    retainerdate,
    updatedate,
    actiondescription,
    userid,
    ROW_NUMBER() OVER (PARTITION BY retainerid ORDER BY updatedate DESC) as row_number
FROM
    {{var('source_database')}}."financial_transactiondetails" ft 
)

SELECT 
    retainer_id AS id,
    lower(
            substring(md5(retainer_id::text || retainer_jobid::text), 1, 8) || '-' ||
            substring(md5(retainer_id::text || retainer_jobid::text), 9, 4) || '-' ||
            substring(md5(retainer_id::text || retainer_jobid::text), 13, 4) || '-' ||
            substring(md5(retainer_id::text || retainer_jobid::text), 17, 4) || '-' ||
            substring(md5(retainer_id::text || retainer_jobid::text), 21, 12)
        ) AS atlas_id,    
    to_char(retainer_date, 'YYYY-MM-DD"T"00:00:00') AS created_at,
    to_char(coalesce(retainer_invoice_paid_date, retainer_date), 'YYYY-MM-DD"T"00:00:00') AS updated_at,
    '{{var('agency_id')}}' AS agency_id,
    if.atlas_id AS fee_type_id,
    retainer_date::DATE AS fee_date,
    COALESCE(retainer_netfee, 0) AS amount,
    'GBP' AS currency,
    ft.actiondescription AS notes,
    fr.retainer_jobid AS project_id,
    ip.atlas_id AS atlas_project_id,
    CASE WHEN retainer_invoice_paid_date NOTNULL THEN 'paid' ELSE 'invoiced' END AS project_fee_status,
    to_char(retainer_date, 'YYYY-MM-DD"T"00:00:00') AS paid_at,
    COALESCE(ft.valueamount, COALESCE(retainer_netfee, 0)) AS default_amount,
    'GBP' AS agency_currency,
    NULL AS person_id,
    to_char(retainer_date, 'YYYY-MM-DD"T"00:00:00') AS invoiced_at,
    COALESCE(ft.userid, '1') AS created_by_id,
    COALESCE(iu.atlas_id, '{{var('master_id')}}') AS created_by_atlas_id
FROM 
    {{var('source_database')}}."financial_retainer" fr
LEFT JOIN 
    internal_fees if ON lower(if.name )= lower(COALESCE(NULLIF(lower(trim(retainer_type::text)), ''), 'unknown_fee'))
LEFT JOIN 
    latest_transaction ft ON ft.retainerid = fr.retainer_id 
    AND ft.row_number = 1
LEFT JOIN 
    internal_projects ip ON ip.id = fr.retainer_jobid
LEFT JOIN
    internal_users iu ON iu.id = ft.userid
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20