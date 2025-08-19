WITH admissions_ranked AS (
    SELECT 
        subject_id,
        hadm_id,
        admittime,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY admittime) AS first_adm_rank,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY admittime DESC) AS last_adm_rank
    FROM hosp.admissions
),
patient_admissions AS (
    SELECT 
        subject_id,
        MAX(CASE WHEN first_adm_rank = 1 THEN hadm_id END) AS first_hadm_id,
        MAX(CASE WHEN last_adm_rank = 1 THEN hadm_id END) AS last_hadm_id
    FROM admissions_ranked
    GROUP BY subject_id
),
first_diagnoses AS (
    SELECT 
        pa.subject_id,
        d.icd_code,
        d.icd_version
    FROM patient_admissions pa
    JOIN hosp.diagnoses_icd d ON pa.first_hadm_id = d.hadm_id
),
last_diagnoses AS (
    SELECT 
        pa.subject_id,
        d.icd_code,
        d.icd_version
    FROM patient_admissions pa
    JOIN hosp.diagnoses_icd d ON pa.last_hadm_id = d.hadm_id
),
common_diagnoses AS (
    SELECT DISTINCT f.subject_id
    FROM first_diagnoses f
    JOIN last_diagnoses l 
        ON f.subject_id = l.subject_id 
        AND f.icd_code = l.icd_code 
        AND f.icd_version = l.icd_version
)
SELECT 
    p.gender,
    ROUND(COUNT(*) * 100.0 / total.total_count, 2) AS percentage
FROM common_diagnoses cd
JOIN hosp.patients p ON cd.subject_id = p.subject_id
CROSS JOIN (SELECT COUNT(*) AS total_count FROM common_diagnoses) total
GROUP BY p.gender, total.total_count
ORDER BY percentage DESC;
