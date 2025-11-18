{{ config(
    materialized='table',
    alias='mapping_resume'
) }}

WITH candidateattachment_filtered AS (
    SELECT
        ca.attachmentid AS id,
        ca.contactid    AS person_id
    FROM {{ var('source_database') }}."candidateattachment" ca
    WHERE ca.type = 'Resume'
),

people_lookup AS (
    SELECT
        p.id,
        p.atlas_id
    FROM "{{ this.schema }}".1_people_ja p
),

attachment_data AS (
    SELECT
        a.candidateattachmentid,
        a.storagename,
        a.filename
    FROM {{ var('source_database') }}."attachment" a
),

final AS (
    SELECT
        -- candidateattachment info
        caf.id              AS attachmentid,
        caf.person_id,
        
        -- Map person_id → atlas_id
        pl.atlas_id         AS atlas_person_id,

        -- File info from the attachment table
        att.storagename     AS file_id,
        REGEXP_REPLACE(att.storagename, '^.*/', '') AS adjusted_file_id,
        att.filename        AS filename  -- <--- ADDED COLUMN

    FROM candidateattachment_filtered AS caf
    LEFT JOIN people_lookup AS pl
           ON caf.person_id = pl.id
    LEFT JOIN attachment_data AS att
           ON caf.id = att.candidateattachmentid
)

SELECT *
FROM final
