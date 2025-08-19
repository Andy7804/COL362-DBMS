WITH admission_transfers AS (
  SELECT
    subject_id,
    hadm_id,
    ARRAY_AGG(transfer_id ORDER BY intime) AS transfers
  FROM hosp.transfers
  WHERE hadm_id IS NOT NULL
  GROUP BY subject_id, hadm_id
),
chain_info AS (
  SELECT
    subject_id,
    hadm_id,
    transfers,
    ARRAY_LENGTH(transfers, 1) AS chain_length
  FROM admission_transfers
),
max_chain AS (
  SELECT MAX(chain_length) AS max_length
  FROM chain_info
),
eligible_subjects AS (
  SELECT DISTINCT subject_id
  FROM chain_info
  WHERE chain_length = (SELECT max_length FROM max_chain)
)
SELECT
  ci.subject_id,
  ci.hadm_id,
  '[' || ARRAY_TO_STRING(ci.transfers, ',') || ']' AS transfers
FROM eligible_subjects es
LEFT OUTER JOIN chain_info ci ON es.subject_id = ci.subject_id
ORDER BY
  ARRAY_LENGTH(ci.transfers, 1) ASC,
  ci.hadm_id ASC,
  ci.subject_id ASC;