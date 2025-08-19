SELECT 
    a.hadm_id, 
    p.gender, 
    CASE 
        WHEN EXTRACT(EPOCH FROM JUSTIFY_INTERVAL(AGE(TO_TIMESTAMP(a.dischtime, 'YYYY-MM-DD HH24:MI:SS'), 
                                                     TO_TIMESTAMP(a.admittime, 'YYYY-MM-DD HH24:MI:SS')))) < 86400 
        THEN TO_CHAR(JUSTIFY_INTERVAL(AGE(TO_TIMESTAMP(a.dischtime, 'YYYY-MM-DD HH24:MI:SS'), 
                                          TO_TIMESTAMP(a.admittime, 'YYYY-MM-DD HH24:MI:SS'))), 
                     'HH24:MI:SS')

        ELSE FLOOR(EXTRACT(EPOCH FROM JUSTIFY_INTERVAL(AGE(TO_TIMESTAMP(a.dischtime, 'YYYY-MM-DD HH24:MI:SS'), 
                                                           TO_TIMESTAMP(a.admittime, 'YYYY-MM-DD HH24:MI:SS')))) / 86400) 
             || ' days, ' || 
             TO_CHAR(JUSTIFY_INTERVAL(AGE(TO_TIMESTAMP(a.dischtime, 'YYYY-MM-DD HH24:MI:SS'), 
                                          TO_TIMESTAMP(a.admittime, 'YYYY-MM-DD HH24:MI:SS'))), 
                     'HH24:MI:SS')
    END AS duration
FROM hosp.admissions a
LEFT OUTER JOIN hosp.patients p ON a.subject_id = p.subject_id
WHERE a.dischtime IS NOT NULL
ORDER BY AGE(TO_TIMESTAMP(a.dischtime, 'YYYY-MM-DD HH24:MI:SS'), 
             TO_TIMESTAMP(a.admittime, 'YYYY-MM-DD HH24:MI:SS')),
         a.hadm_id;