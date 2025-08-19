SELECT DISTINCT pharmacy_id
FROM hosp.pharmacy
EXCEPT
SELECT DISTINCT pharmacy_id
FROM hosp.prescriptions
ORDER BY pharmacy_id;