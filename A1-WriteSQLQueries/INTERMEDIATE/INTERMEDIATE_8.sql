WITH admission_durations AS (
  SELECT 
    subject_id,
    TO_TIMESTAMP(dischtime, 'YYYY-MM-DD HH24:MI:SS') - 
    TO_TIMESTAMP(admittime, 'YYYY-MM-DD HH24:MI:SS') as duration
  FROM hosp.admissions
  WHERE dischtime IS NOT NULL 
    AND admittime IS NOT NULL
)
SELECT 
  subject_id,
  JUSTIFY_INTERVAL(AVG(duration)) as avg_duration
FROM admission_durations
GROUP BY subject_id
ORDER BY subject_id;