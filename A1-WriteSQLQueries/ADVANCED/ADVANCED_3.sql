WITH selected_patients AS (
	SELECT DISTINCT(subject_id)
	FROM hosp.procedures_icd hpi
	GROUP BY subject_id, hadm_id
	HAVING COUNT(DISTINCT icd_code) > 1
	INTERSECT
	SELECT DISTINCT (subject_id)
	FROM hosp.diagnoses_icd
	WHERE icd_code LIKE 'T81%' 
),
distinct_proc AS (
	SELECT DISTINCT(subject_id), COUNT(DISTINCT icd_code) AS dist_proc_count
	FROM hosp.procedures_icd hpi
	GROUP BY subject_id
),
patient_admissions AS (
	SELECT 
		ha.subject_id,
        ha.hadm_id,
        COUNT(ht.transfer_id) AS transfers_per_admission
	FROM hosp.admissions ha 
	LEFT OUTER JOIN hosp.transfers ht ON ha.subject_id = ht.subject_id AND ha.hadm_id = ht.hadm_id
	WHERE ha.subject_id in (SELECT subject_id FROM selected_patients)
	GROUP BY ha.subject_id, ha.hadm_id
),
transfer_stats AS (
	SELECT 
		subject_id, 
		COUNT(DISTINCT hadm_id) AS total_admissions, 
		SUM(transfers_per_admission) AS total_transfers
	FROM patient_admissions
	GROUP BY subject_id
),
patient_avg AS (
    SELECT 
        subject_id,
        total_transfers::FLOAT / total_admissions AS average_transfers
    FROM transfer_stats
),
overall_avg AS (
    SELECT AVG(average_transfers) AS o_avg
    FROM patient_avg
)
SELECT 
	pa.subject_id,
	dp.dist_proc_count,
	pa.average_transfers
FROM patient_avg pa
LEFT OUTER JOIN distinct_proc dp ON pa.subject_id = dp.subject_id
CROSS JOIN overall_avg oa
WHERE pa.average_transfers >= oa.o_avg
ORDER BY pa.average_transfers DESC, dp.dist_proc_count DESC, pa.subject_id;