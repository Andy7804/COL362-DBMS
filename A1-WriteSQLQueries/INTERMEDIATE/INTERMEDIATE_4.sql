WITH infection_admissions AS (
    SELECT DISTINCT
        ha.subject_id,
        ha.hadm_id,
        EXTRACT(YEAR FROM TO_TIMESTAMP(admittime, 'YYYY-MM-DD HH24:MI:SS')) AS "year"
    FROM hosp.admissions ha 
    LEFT OUTER JOIN hosp.diagnoses_icd hdi ON (ha.hadm_id = hdi.hadm_id AND ha.subject_id = hdi.subject_id)
    LEFT OUTER JOIN hosp.d_icd_diagnoses hdid ON (hdi.icd_code = hdid.icd_code AND hdi.icd_version = hdid.icd_version)
    WHERE hdid.long_title ILIKE '%infection%'
)
SELECT 
    subject_id,
    COUNT(DISTINCT hadm_id) AS count_admissions,
    year
FROM infection_admissions
GROUP BY subject_id, "year"
HAVING COUNT(DISTINCT hadm_id) > 1
ORDER BY "year", count_admissions DESC;