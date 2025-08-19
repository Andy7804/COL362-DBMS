SELECT COUNT(DISTINCT ha.hadm_id)
FROM hosp.admissions ha
JOIN hosp.emar he ON ha.hadm_id = he.hadm_id
JOIN hosp.emar_detail hd ON he.emar_id = hd.emar_id
WHERE reason_for_no_barcode = 'Barcode Damaged' AND marital_status <> 'MARRIED';