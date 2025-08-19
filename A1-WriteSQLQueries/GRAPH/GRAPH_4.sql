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
connected_nodes AS (
    SELECT subject_id2 AS node, ARRAY[10038081, subject_id2] AS path
    FROM graph2
    WHERE subject_id1 = 10038081

    UNION ALL

    SELECT g.subject_id2, cn.path || g.subject_id2
    FROM connected_nodes cn
    JOIN graph2 g ON cn.node = g.subject_id1
    WHERE g.subject_id2 <> ALL(cn.path)
)
SELECT COUNT(DISTINCT node) AS count
FROM connected_nodes;