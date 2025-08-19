WITH DrugPatients AS (
    SELECT DISTINCT
        subject_id,
        hadm_id,
        CASE 
            WHEN MAX(CASE WHEN LOWER(drug) LIKE '%amlodipine%' THEN 1 ELSE 0 END) = 1 
                AND MAX(CASE WHEN LOWER(drug) LIKE '%lisinopril%' THEN 1 ELSE 0 END) = 1 THEN 'both'
            WHEN MAX(CASE WHEN LOWER(drug) LIKE '%amlodipine%' THEN 1 ELSE 0 END) = 1 THEN 'amlodipine'
            WHEN MAX(CASE WHEN LOWER(drug) LIKE '%lisinopril%' THEN 1 ELSE 0 END) = 1 THEN 'lisinopril'
        END as drug
    FROM hosp.prescriptions
    WHERE LOWER(drug) LIKE '%amlodipine%'
        OR LOWER(drug) LIKE '%lisinopril%'
    GROUP BY subject_id, hadm_id
),

ServicePaths AS (
    SELECT 
        subject_id,
        hadm_id,
        curr_service as initial_service,
        transfertime
    FROM hosp.services
    WHERE prev_service IS NULL
),

ServiceSequence AS (
    WITH RECURSIVE ServiceChain AS (
        SELECT 
            s.subject_id,
            s.hadm_id,
            s.curr_service as service,
            s.transfertime,
            ARRAY[s.curr_service] as service_path
        FROM hosp.services s
        WHERE s.prev_service IS NULL

        UNION ALL

        SELECT 
            s.subject_id,
            s.hadm_id,
            s.curr_service,
            s.transfertime,
            sc.service_path || s.curr_service
        FROM hosp.services s
        JOIN ServiceChain sc ON 
            s.subject_id = sc.subject_id 
            AND s.hadm_id = sc.hadm_id
            AND s.prev_service = sc.service
            AND s.transfertime > sc.transfertime
    )
    SELECT DISTINCT ON (subject_id, hadm_id)
        subject_id,
        hadm_id,
        service_path as services
    FROM ServiceChain
    ORDER BY subject_id, hadm_id, transfertime DESC
)

SELECT 
    d.subject_id,
    d.hadm_id,
    d.drug,
    COALESCE(ss.services, ARRAY[]::text[]) as services
FROM DrugPatients d
LEFT JOIN ServiceSequence ss ON 
    d.subject_id = ss.subject_id 
    AND d.hadm_id = ss.hadm_id
ORDER BY 
    d.subject_id,
    d.hadm_id;