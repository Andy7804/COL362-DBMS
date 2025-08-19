WITH resistant_antibiotics AS (
    SELECT 
        subject_id, 
        hadm_id, 
        COUNT(DISTINCT micro_specimen_id) AS resistant_antibiotic_count
    FROM hosp.microbiologyevents
    WHERE interpretation = 'R' AND hadm_id IS NOT NULL
    GROUP BY subject_id, hadm_id
    HAVING COUNT(DISTINCT micro_specimen_id) >= 2
),
icu_stays AS (
    SELECT 
        subject_id, 
        hadm_id, 
        ROUND(CAST(SUM(los * 24) AS NUMERIC), 2) AS icu_length_of_stay_hours
    FROM icu.icustays
    GROUP BY subject_id, hadm_id
)
SELECT 
    ha.subject_id,
    ha.hadm_id,
    COALESCE(ra.resistant_antibiotic_count, 0) AS resistant_antibiotic_count,
    COALESCE(icu.icu_length_of_stay_hours, 0) AS icu_length_of_stay_hours,
    CASE WHEN ha.discharge_location = 'DIED' THEN 1 ELSE 0 END AS died_in_hospital
FROM hosp.admissions ha
LEFT JOIN resistant_antibiotics ra ON ha.subject_id = ra.subject_id AND ha.hadm_id = ra.hadm_id
LEFT JOIN icu_stays icu ON ha.subject_id = icu.subject_id AND ha.hadm_id = icu.hadm_id
WHERE ra.subject_id IS NOT NULL  
ORDER BY 
    died_in_hospital DESC,
    resistant_antibiotic_count DESC,
    icu_length_of_stay_hours DESC,
    ha.subject_id,
    ha.hadm_id
