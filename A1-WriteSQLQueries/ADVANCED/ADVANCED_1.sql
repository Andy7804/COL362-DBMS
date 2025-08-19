WITH diagnosis_info AS (
    SELECT 
        hdi.subject_id,
        hdi.hadm_id,
        ARRAY_AGG(DISTINCT hdi.icd_code ORDER BY hdi.icd_code) AS diagnosis_set
    FROM hosp.diagnoses_icd hdi
    GROUP BY hdi.subject_id, hdi.hadm_id
),
medications_info AS (
    SELECT 
        hp.subject_id,
        hp.hadm_id,
        ARRAY_AGG(DISTINCT hp.drug ORDER BY hp.drug) AS medication_set
    FROM hosp.prescriptions hp
    GROUP BY hp.subject_id, hp.hadm_id
),
combined_admission_data AS (
    SELECT 
        ha.subject_id,
        ha.hadm_id,
        dinfo.diagnosis_set,
        minfo.medication_set
    FROM hosp.admissions ha
    LEFT JOIN diagnosis_info dinfo 
        ON ha.subject_id = dinfo.subject_id AND ha.hadm_id = dinfo.hadm_id
    LEFT JOIN medications_info minfo 
        ON ha.subject_id = minfo.subject_id AND ha.hadm_id = minfo.hadm_id
),
patient_stats AS (
    SELECT
        cad.subject_id,
        COUNT(DISTINCT cad.hadm_id) AS total_admissions,
        COUNT(DISTINCT COALESCE(cad.diagnosis_set, ARRAY[]::text[])) AS num_distinct_diagnoses_set_count,
        COUNT(DISTINCT COALESCE(cad.medication_set, ARRAY[]::text[])) AS num_distinct_medications_set_count
    FROM combined_admission_data cad
    GROUP BY cad.subject_id
    HAVING 
        COUNT(DISTINCT cad.hadm_id) >= 3 AND (
            COUNT(DISTINCT COALESCE(cad.diagnosis_set, ARRAY[]::text[])) >= 3 OR
            COUNT(DISTINCT COALESCE(cad.medication_set, ARRAY[]::text[])) >= 3
        )
)
SELECT 
    ps.subject_id,
    ps.total_admissions,
    ps.num_distinct_diagnoses_set_count,
    ps.num_distinct_medications_set_count
FROM patient_stats ps
ORDER BY 
    ps.total_admissions DESC,
    ps.num_distinct_diagnoses_set_count DESC,
    ps.subject_id;