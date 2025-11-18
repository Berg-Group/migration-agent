{{ config(materialized='table', alias='user_mapping_blackwood', tags=['blackwood']) }}

SELECT '73882713-50bc-4557-842a-c361003472e2' AS id, 'BWG Atlas'             AS "name", 'bwgatlas@blackwoodgroup.com'               AS email, 'active' AS status
UNION ALL SELECT '9b8fbe30-94e1-4517-88a8-aa02ebcda1f9', 'Terry Wells'          , 'terry.wells@blackwoodgroup.com'            , 'active'
UNION ALL SELECT '755811fd-da49-4763-b34a-36fab86a46a2', 'Christian Summerfield', 'christian.summerfield@blackwoodgroup.com' , 'active'
UNION ALL SELECT 'fb119853-93ca-492e-b1d7-e5d840e353f7', 'Archie Brown'         , 'archie.brown@blackwoodgroup.com'           , 'active'
UNION ALL SELECT 'd9258e65-c04e-4261-b73c-94095505dec0', 'Rupert Milne'         , 'rupert.milne@blackwoodgroup.com'           , 'active'
UNION ALL SELECT 'f444e4a8-9913-4a87-8d3f-ec425da2e4e4', 'AI blackwoodgroupcom' , 'ai.blackwoodgroupcom@recruitwithatlas.com' , 'AI'
UNION ALL SELECT 'e4cc5c77-0b1d-4e0d-8411-7c5e1fd68740', 'Lisa Wilkinson'       , 'lisa.wilkinson@blackwoodgroup.com'         , 'active'
UNION ALL SELECT '5998212b-12fc-4194-b2cd-1125056ff5d7', 'Joel Holford'         , 'joel.holford@blackwoodgroup.com'           , 'active'
UNION ALL SELECT 'f9141ef0-8723-483a-aed2-0f9266e61d9b', 'Patrick Hayes'        , 'patrick.hayes@blackwoodgroup.com'          , 'active'
UNION ALL SELECT '6f9fbd69-e740-4175-9552-6a5f9f786c26', 'Emma Lanigan'         , 'emma.lanigan@blackwoodgroup.com'           , 'active'
UNION ALL SELECT 'c8d68a99-a74f-4e98-9f94-5c15bc3eb67f', 'Charlotte Baxendale'  , 'charlotte.baxendale@blackwoodgroup.com'    , 'active'
