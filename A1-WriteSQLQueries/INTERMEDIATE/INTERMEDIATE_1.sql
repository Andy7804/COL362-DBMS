SELECT DISTINCT ON (ha.subject_id) 
       ha.subject_id, 
       ha.hadm_id AS hadm_id, 
       hp.dod
FROM hosp.admissions ha 
JOIN hosp.patients hp on ha.subject_id = hp.subject_id 
WHERE hp.dod IS NOT NULL
ORDER BY ha.subject_id, ha.admittime;