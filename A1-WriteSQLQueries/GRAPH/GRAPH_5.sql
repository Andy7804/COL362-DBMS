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
shortest_paths AS (
    SELECT 
        CAST(10037861 AS bigint) AS start_id,
        CAST(10037861 AS bigint) AS connected_id,
        0 AS path_length,
        ARRAY[CAST(10037861 AS bigint)] AS path
    
    UNION ALL
    
    SELECT 
        sp.start_id,
        g.subject_id2,
        sp.path_length + 1,
        sp.path || g.subject_id2
    FROM shortest_paths sp
    JOIN graph2 g ON sp.connected_id = g.subject_id1
    WHERE NOT g.subject_id2 = ANY(sp.path)
      AND sp.path_length < (SELECT COUNT(*) FROM earliest_admissions) - 1
),
min_paths AS (
    SELECT 
        start_id,
        connected_id,
        MIN(path_length) AS path_length
    FROM shortest_paths
    WHERE start_id <> connected_id
    GROUP BY start_id, connected_id
)
SELECT 
    start_id,
    connected_id,
    path_length
FROM min_paths
ORDER BY path_length, connected_id;
