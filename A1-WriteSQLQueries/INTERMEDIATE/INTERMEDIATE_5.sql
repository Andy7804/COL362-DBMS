WITH hosp_events AS (
SELECT subject_id, hadm_id, 'diagnosis' AS event_type, COUNT(*) AS event_count
FROM hosp.diagnoses_icd
GROUP BY subject_id, hadm_id
UNION ALL
SELECT subject_id, hadm_id, 'procedure' AS event_type, COUNT(*) AS event_count
FROM hosp.procedures_icd
GROUP BY subject_id, hadm_id
)
SELECT ha.subject_id, ha.hadm_id,
	COALESCE(SUM(CASE WHEN event_type = 'procedure' THEN event_count END), 0) AS count_procedures,
	COALESCE(SUM(CASE WHEN event_type = 'diagnosis' THEN event_count END), 0) AS count_diagnoses
FROM hosp.admissions ha
LEFT OUTER JOIN hosp_events he ON (ha.subject_id = he.subject_id AND ha.hadm_id = he.hadm_id)
WHERE ha.admission_type = 'URGENT' AND ha.hospital_expire_flag = 1
GROUP BY ha.subject_id, ha.hadm_id
ORDER BY subject_id, hadm_id, count_procedures DESC, count_diagnoses DESC;