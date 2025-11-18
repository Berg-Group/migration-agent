{{ config(
    materialized='table',           
    alias='email_to_cc_role',      
    tags=['ryton_migration']
) }}

with base as (
    select
        email,
        job_title
    from {{ var('source_database') }}.public_contact
    where lower(job_title) like '%head of%'
       or lower(job_title) like '%director%'
)

select
    email,
    job_title
from base