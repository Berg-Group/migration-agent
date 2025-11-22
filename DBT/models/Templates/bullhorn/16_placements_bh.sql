{{ config(
    materialized = 'table',
    alias        = 'placements_bh',
    tags         = ['bullhorn']
) }}

SELECT
    f.project_id,
    f.atlas_project_id,
    c.id AS candidate_id,
    c.atlas_id AS atlas_candidate_id,
    f.id AS project_fee_id,
    f.atlas_id AS atlas_project_fee_id
FROM {{ ref('15_project_fees_bh') }} f
INNER JOIN {{ ref('12_candidates_bh') }} c 
    ON c.project_id = f.project_id 
   AND c.person_id = f.candidate_id