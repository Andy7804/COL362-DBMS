WITH I2_first_adm AS (
    SELECT 
        d.subject_id, 
        a.hadm_id AS first_hadm_id,
        a.admittime AS first_admittime, 
        a.dischtime AS first_dischtime
    FROM hosp.diagnoses_icd d
    JOIN hosp.admissions a ON d.hadm_id = a.hadm_id
    WHERE d.icd_code LIKE 'I2%' 
    AND a.admittime = (SELECT MIN(admittime) FROM hosp.admissions WHERE subject_id = d.subject_id)
),
RankedAdmissions AS (
    SELECT 
        subject_id,
        hadm_id,
        admittime,
        dischtime,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY admittime) as admission_number,
        LEAD(hadm_id) OVER (PARTITION BY subject_id ORDER BY admittime) as second_hadm_id,
        LEAD(admittime) OVER (PARTITION BY subject_id ORDER BY admittime) as second_admittime,
        LEAD(dischtime) OVER (PARTITION BY subject_id ORDER BY admittime) as second_dischtime
    FROM hosp.admissions
),
Interval_satisfy AS (
	SELECT 
	    subject_id,
	    hadm_id,
	    second_hadm_id,
	    CAST(admittime AS TIMESTAMP) as first_admittime,
	    CAST(dischtime AS TIMESTAMP) as first_dischtime,
	    CAST(second_admittime AS TIMESTAMP) as second_admittime,
	    CAST(second_dischtime AS TIMESTAMP) as second_dischtime,
	    TO_CHAR(AGE(CAST(second_admittime AS TIMESTAMP), CAST(dischtime AS TIMESTAMP)), 'YYYY-MM-DD HH24:MI:SS') 
	        as time_gap_between_admissions
	FROM RankedAdmissions
	WHERE admission_number = 1
	AND CAST(second_admittime AS TIMESTAMP) - CAST(dischtime AS TIMESTAMP) < INTERVAL '180 days'
),
table1 AS (
	SELECT a.subject_id, second_hadm_id, time_gap_between_admissions
	FROM I2_first_adm a
	INNER JOIN Interval_satisfy b ON a.subject_id = b.subject_id  
),
Services AS (
    SELECT 
        t.subject_id, 
        t.second_hadm_id,
        '{' || STRING_AGG(s.curr_service, ',' ORDER BY s.transfertime) || '}' AS services
    FROM table1 t
    JOIN hosp.services s ON t.second_hadm_id = s.hadm_id
    GROUP BY t.subject_id, t.second_hadm_id
)
SELECT 
    t.subject_id, 
    t.second_hadm_id, 
    t.time_gap_between_admissions, 
    COALESCE(s.services, '{}') AS services
FROM table1 t
LEFT JOIN Services s ON t.subject_id = s.subject_id AND t.second_hadm_id = s.second_hadm_id
ORDER BY LENGTH(services) DESC, t.time_gap_between_admissions DESC, t.subject_id, t.second_hadm_id;