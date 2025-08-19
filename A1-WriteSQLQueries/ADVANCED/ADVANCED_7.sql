WITH DiabetesPatients AS (
    SELECT DISTINCT
        subject_id,
        hadm_id
    FROM hosp.diagnoses_icd
    WHERE icd_code LIKE 'E10%' OR icd_code LIKE 'E11%'
),
KidneyPatients AS (
    SELECT DISTINCT
        subject_id,
        hadm_id
    FROM hosp.diagnoses_icd
    WHERE icd_code LIKE 'N18%'
),
QualifiedPatients AS (
    SELECT DISTINCT
        d.subject_id
    FROM DiabetesPatients d
    JOIN KidneyPatients k ON d.subject_id = k.subject_id
    JOIN hosp.admissions a1 ON d.hadm_id = a1.hadm_id
    JOIN hosp.admissions a2 ON k.hadm_id = a2.hadm_id
    WHERE a1.admittime <= a2.admittime 
),
AllDiagnoses AS (
    SELECT 
        d.subject_id,
        d.hadm_id as admission_id,
        'diagnoses' as diagnoses_or_procedure,
        d.icd_code
    FROM hosp.diagnoses_icd d
    JOIN QualifiedPatients qp ON d.subject_id = qp.subject_id
),
AllProcedures AS (
    SELECT 
        p.subject_id,
        p.hadm_id as admission_id,
        'procedures' as diagnoses_or_procedure,
        p.icd_code
    FROM hosp.procedures_icd p
    JOIN QualifiedPatients qp ON p.subject_id = qp.subject_id
),
CombinedCodes AS (
    SELECT * FROM AllDiagnoses
    UNION ALL
    SELECT * FROM AllProcedures
)
SELECT DISTINCT
    subject_id,
    admission_id,
    diagnoses_or_procedure,
    icd_code
FROM CombinedCodes
ORDER BY 
    subject_id ASC,
    admission_id ASC,
    icd_code ASC,
    diagnoses_or_procedure ASC;