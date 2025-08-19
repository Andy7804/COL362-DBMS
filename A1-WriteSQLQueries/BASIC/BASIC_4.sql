SELECT enter_provider_id, COUNT(DISTINCT medication)
FROM hosp.emar
WHERE enter_provider_id IS NOT NULL
GROUP BY enter_provider_id
ORDER BY COUNT(DISTINCT medication) DESC;