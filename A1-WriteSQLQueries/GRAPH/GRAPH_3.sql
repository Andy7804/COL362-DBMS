WITH RECURSIVE earliest_admissions AS (
    SELECT subject_id, hadm_id, admittime, dischtime
    FROM hosp.admissions
    ORDER BY admittime
    LIMIT 200
),
graph2 AS (
    SELECT 
        a.subject_id AS subject_id1,
        b.subject_id AS subject_id2
    FROM earliest_admissions a
    JOIN earliest_admissions b
        ON a.subject_id <> b.subject_id
        AND a.admittime < b.dischtime
        AND a.dischtime > b.admittime
),
shortest_path AS (
    SELECT subject_id1 AS node, 0 AS path_length, ARRAY[subject_id1] AS path
    FROM graph2
    WHERE subject_id1 = 10038081

    UNION ALL

    SELECT g.subject_id2, sp.path_length + 1, sp.path || g.subject_id2
    FROM shortest_path sp
    JOIN graph2 g ON sp.node = g.subject_id1
    WHERE g.subject_id2 <> ALL(sp.path)
)
SELECT path_length
FROM shortest_path
WHERE node = 10021487
ORDER BY path_length
LIMIT 1