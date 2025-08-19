WITH filtered_procedures AS (
    SELECT DISTINCT 
        subject_id,
        hadm_id,
        chartdate,
        icd_code
    FROM hosp.procedures_icd
    WHERE icd_code SIMILAR TO '[0-2]%'
),
valid_med_procedures AS (
    SELECT DISTINCT
        p.subject_id,
        p.hadm_id
    FROM filtered_procedures p
    JOIN hosp.prescriptions pr
        ON p.subject_id = pr.subject_id 
        AND p.hadm_id = pr.hadm_id
        AND pr.starttime::date BETWEEN CAST(p.chartdate AS DATE) 
                                    AND (CAST(p.chartdate AS DATE) + INTERVAL '1 day')
),
patients_with_multiple_meds AS (
    SELECT DISTINCT
        subject_id,
        hadm_id
    FROM hosp.prescriptions
    GROUP BY subject_id, hadm_id
    HAVING COUNT(DISTINCT drug) >= 2
),
eligible_cases AS (
    SELECT 
        v.subject_id,
        v.hadm_id
    FROM valid_med_procedures v
    JOIN patients_with_multiple_meds p
        ON v.subject_id = p.subject_id 
        AND v.hadm_id = p.hadm_id
),
time_calculations AS (
    SELECT 
        e.subject_id,
        e.hadm_id,
        (MIN(CAST(p.chartdate AS DATE)) || ' 00:00:00')::timestamp AS first_proc_time,
        MAX(CAST(pr.starttime AS TIMESTAMP)) AS last_med_time
    FROM eligible_cases e
    JOIN hosp.procedures_icd p
        ON e.subject_id = p.subject_id 
        AND e.hadm_id = p.hadm_id
    JOIN hosp.prescriptions pr
        ON e.subject_id = pr.subject_id 
        AND e.hadm_id = pr.hadm_id
    GROUP BY e.subject_id, e.hadm_id
)
SELECT 
    t.subject_id,
    t.hadm_id,
    COUNT(DISTINCT d.icd_code) AS distinct_diagnoses,
    COUNT(DISTINCT p.icd_code) AS distinct_procedures,
    TO_CHAR(AGE(t.last_med_time, t.first_proc_time), 'YYYY-MM-DD HH24:MI:SS') AS time_gap
FROM time_calculations t
JOIN hosp.diagnoses_icd d
    ON t.subject_id = d.subject_id 
    AND t.hadm_id = d.hadm_id
JOIN hosp.procedures_icd p
    ON t.subject_id = p.subject_id 
    AND t.hadm_id = p.hadm_id
GROUP BY 
    t.subject_id,
    t.hadm_id,
    t.first_proc_time,
    t.last_med_time
ORDER BY 
    distinct_diagnoses DESC,
    distinct_procedures DESC,
    time_gap,
    t.subject_id,
    t.hadm_id;