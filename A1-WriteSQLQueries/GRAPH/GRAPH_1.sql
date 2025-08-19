WITH earliest_admissions AS (
    SELECT subject_id, hadm_id, admittime, dischtime
    FROM hosp.admissions
    ORDER BY admittime
    LIMIT 200
),
overlapping_pairs AS (
    SELECT 
        a.subject_id AS subject_id1,
        b.subject_id AS subject_id2,
        a.hadm_id AS hadm_id1,
        b.hadm_id AS hadm_id2
    FROM earliest_admissions a
    JOIN earliest_admissions b
        ON a.subject_id < b.subject_id
        AND a.admittime < b.dischtime
        AND a.dischtime > b.admittime
),
shared_diagnoses AS (
    SELECT DISTINCT
        op.subject_id1,
        op.subject_id2
    FROM overlapping_pairs op
    WHERE EXISTS (
        SELECT 1
        FROM hosp.diagnoses_icd d1
        JOIN hosp.diagnoses_icd d2 
            ON d1.icd_code = d2.icd_code 
            AND d1.icd_version = d2.icd_version
        WHERE d1.hadm_id = op.hadm_id1
            AND d2.hadm_id = op.hadm_id2
    )
)
SELECT 
    subject_id1,
    subject_id2
FROM shared_diagnoses
ORDER BY subject_id1, subject_id2;