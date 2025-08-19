SELECT COUNT(DISTINCT subject_id), EXTRACT(YEAR FROM TO_TIMESTAMP(admittime, 'YYYY-MM-DD HH24:MI:SS')) AS "year" 
FROM hosp.admissions
GROUP BY "year"
ORDER BY COUNT(DISTINCT subject_id) DESC, "year"
LIMIT 5;