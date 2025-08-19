SELECT hp.subject_id, COUNT(*)
FROM hosp.patients hp 
LEFT OUTER JOIN icu.icustays icus ON hp.subject_id = icus.subject_id
GROUP BY hp.subject_id
ORDER BY count, subject_id;