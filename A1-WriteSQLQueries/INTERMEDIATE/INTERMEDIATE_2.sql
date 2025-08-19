SELECT JUSTIFY_INTERVAL(AVG(
    TO_TIMESTAMP(dischtime, 'YYYY-MM-DD HH24:MI:SS') - 
    TO_TIMESTAMP(admittime, 'YYYY-MM-DD HH24:MI:SS')
)) AS avg_duration
FROM hosp.admissions ha
RIGHT OUTER JOIN hosp.diagnoses_icd hdi 
    ON ha.hadm_id = hdi.hadm_id
WHERE hdi.icd_code = '4019' 
AND hdi.icd_version = '9';
