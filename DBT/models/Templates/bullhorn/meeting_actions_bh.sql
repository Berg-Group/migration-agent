{{ config(
    materialized='table',
    alias='meeting_actions_bh',
    tags = ["bullhorn"]
) }}

SELECT 'candidate_action' AS alias, 'Candidate Call' AS value
UNION ALL
SELECT 'candidate_action', 'Candidate Qualification Call'
UNION ALL
SELECT 'candidate_action', 'Placement Check-In'
UNION ALL
SELECT 'client_action', 'Account Development Call'
UNION ALL
SELECT 'client_action', 'Interview/CV Feedback'
UNION ALL
SELECT 'client_action', 'Client Call'
UNION ALL
SELECT 'client_action', 'Tradeshow Meeting - AD'
UNION ALL
SELECT 'client_action', 'F2F Meeting AD'
UNION ALL
SELECT 'client_action', 'Service Review Call'
UNION ALL
SELECT 'client_action', 'Job Qualification Call'
UNION ALL
SELECT 'client_action', 'Meeting/Conference Notes'
UNION ALL
SELECT 'client_action', 'Exec Search BD Call'
UNION ALL
SELECT 'client_action', 'Exec Search AD Call'
UNION ALL
SELECT 'client_action', 'Spec CV Sent'
UNION ALL
SELECT 'client_action', 'Podcast Recorded'
UNION ALL
SELECT 'client_action', 'Contract update/negotiation'
UNION ALL
SELECT 'target_action', 'Proposal Agreed'
UNION ALL
SELECT 'target_action', 'Proposal Sent'
UNION ALL
SELECT 'target_action', 'BD/Networking Call'
UNION ALL
SELECT 'target_action', 'Tradeshow Meeting - BD'
UNION ALL
SELECT 'target_action', 'F2F Meeting BD'
UNION ALL
SELECT 'target_action', 'Terms Agreed'
UNION ALL
SELECT 'target_action', 'Terms Sent'