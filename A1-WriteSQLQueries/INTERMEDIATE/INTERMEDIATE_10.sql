WITH first_admissions AS (
    SELECT subject_id, MIN(admittime) AS first_admittime
    FROM hosp.admissions
    GROUP BY subject_id
),
kidney_patients AS (
    SELECT DISTINCT a.subject_id, a.admittime
    FROM hosp.admissions a
    JOIN first_admissions fa ON a.subject_id = fa.subject_id AND a.admittime = fa.first_admittime
    JOIN hosp.diagnoses_icd d ON a.hadm_id = d.hadm_id
    JOIN hosp.d_icd_diagnoses icd ON d.icd_code = icd.icd_code AND d.icd_version = icd.icd_version
    WHERE icd.long_title ILIKE '%kidney%'
),
multiple_admissions AS (
    SELECT subject_id, MIN(admittime) AS admittime
    FROM hosp.admissions
    GROUP BY subject_id 
    HAVING COUNT(*) > 1
),
outer_table AS (
	SELECT kp.subject_id, kp.admittime
	FROM kidney_patients kp
	INNER JOIN multiple_admissions ma ON kp.subject_id = ma.subject_id AND kp.admittime = ma.admittime
	ORDER BY kp.admittime DESC
)
SELECT subject_id
FROM outer_table
ORDER BY subject_id
LIMIT 100