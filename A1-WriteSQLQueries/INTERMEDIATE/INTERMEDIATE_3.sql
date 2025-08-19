WITH event_counts AS (
    SELECT caregiver_id, 'procedure' AS event_type, COUNT(*) AS event_count
    FROM icu.procedureevents
    GROUP BY caregiver_id
    UNION ALL
    SELECT caregiver_id, 'chart' AS event_type, COUNT(*) AS event_count
    FROM icu.chartevents
    GROUP BY caregiver_id
    UNION ALL
    SELECT caregiver_id, 'datetime' AS event_type, COUNT(*) AS event_count
    FROM icu.datetimeevents
    GROUP BY caregiver_id
)
SELECT 
    c.caregiver_id,
    COALESCE(SUM(CASE WHEN event_type = 'procedure' THEN event_count END), 0) AS procedureevents_count,
    COALESCE(SUM(CASE WHEN event_type = 'chart' THEN event_count END), 0) AS chartevents_count,
    COALESCE(SUM(CASE WHEN event_type = 'datetime' THEN event_count END), 0) AS datetimeevents_count
FROM 
    icu.caregiver c
LEFT OUTER JOIN event_counts ec ON c.caregiver_id = ec.caregiver_id
GROUP BY c.caregiver_id
ORDER BY 
    c.caregiver_id, 
    procedureevents_count, 
    chartevents_count, 
    datetimeevents_count;