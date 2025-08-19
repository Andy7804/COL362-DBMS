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
        ON a.subject_id <> b.subject_id
        AND a.admittime < b.dischtime
        AND a.dischtime > b.admittime
),
graph2 AS (
    SELECT 
        subject_id1,
        subject_id2
    FROM overlapping_pairs
    ORDER BY subject_id1, subject_id2
),
edge_check AS (
    SELECT 
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM graph2 
                WHERE (subject_id1 = 10006580 AND subject_id2 = 10003400))
            THEN 1
            ELSE 0
        END AS path_exists
)
SELECT path_exists FROM edge_check;