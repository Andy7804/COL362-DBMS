WITH subjects as (
SELECT DISTINCT ha.subject_id
FROM hosp.admissions ha
LEFT OUTER JOIN hosp.prescriptions hp ON ha.subject_id = hp.subject_id AND ha.hadm_id = hp.hadm_id
WHERE drug = 'OxyCODONE (Immediate Release)'
INTERSECT
SELECT DISTINCT ha.subject_id
FROM hosp.admissions ha
LEFT OUTER JOIN hosp.prescriptions hp ON ha.subject_id = hp.subject_id AND ha.hadm_id = hp.hadm_id
WHERE drug = 'Insulin'
)
SELECT ROUND(AVG(CAST(result_value AS DECIMAL(10,2))),10) AS "avg_BMI"
FROM subjects 
LEFT OUTER JOIN hosp.omr ON subjects.subject_id = hosp.omr.subject_id
WHERE result_name = 'BMI (kg/m2)';