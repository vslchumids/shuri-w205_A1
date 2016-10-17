----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_measure.sql
-- Description: SQL Script to transform raw data table measure_date into 
--              the measure table for data investigation purposes.
-- Date       : 10/7/2016
----------------------------------------------------------------------------
DROP TABLE measure;

CREATE TABLE measure AS
SELECT DISTINCT measure_id, 
                measure_name, 
                procedure_id, 
                IF(procedure_id IN ('AMI', 'CAC', 'HF', 'IMM', 'PN', 'SCIP', 'STK', 'PCH') 
                   OR measure_id IN ('HBIPS-5', 'HBIPS-6', 'HBIPS-7', 
                                     'OP_2', 'OP_23', 'OP_4', 'OP_6', 'OP_7', 
                                     'VTE_1', 'VTE_2', 'VTE_3', 'VTE_4', 'VTE_5'),
                   'H', 
                   IF(procedure_id IN ('COMP_HIP_KNEE', 'MORT', 'READM', 'ED', 'PC') 
                      OR measure_id IN ('HAI_1_SIR', 'HAI_2_SIR', 'HAI_3_SIR', 'HAI_4_SIR', 'HAI_5_SIR', 'HAI_6_SIR', 
                                        'HBIPS-2', 'HBIPS-3', 'HBIPS-4', 
                                        'PSI_4_SURG_COMP', 'PSI_90_SAFETY', 
                                        'OP_1', 'OP_3b', 'OP_5', 'OP_8', 'OP_9', 'OP_10', 
                                        'OP_11', 'OP_13', 'OP_14', 'OP_18b', 'OP_20', 'OP_21', 'OP_22', 
                                        'VTE_6'),
                      'L', NULL)) AS scoring_system
FROM 
(SELECT m1.measure_id, 
NVL(md.measure_name, m1.measure_id) AS measure_name,
CASE REGEXP_EXTRACT(m1.measure_id, '([A-Z]+)_(.*[0-9]+.*)', 1) 
WHEN '' THEN 
CASE REGEXP_EXTRACT(m1.measure_id, '([A-Z]+)-(.*[0-9]+.*)', 1)
WHEN '' THEN
m1.measure_id 
ELSE REGEXP_EXTRACT(m1.measure_id, '([A-Z]+)-(.*[0-9]+.*)', 1) END
ELSE REGEXP_EXTRACT(m1.measure_id, '([A-Z]+)_(.*[0-9]+.*)', 1) 
END AS procedure_id
FROM (
SELECT measure_id
FROM complication
UNION ALL
SELECT measure_id
FROM infection
UNION ALL
SELECT measure_id
FROM outpatient_imaging
UNION ALL
SELECT measure_id
FROM readmission_death
UNION ALL
SELECT measure_id
FROM effective_care
UNION ALL
SELECT measure_id
FROM cancer
UNION ALL
SELECT 'HBIPS-2' AS measure_id 
UNION ALL
SELECT 'HBIPS-3' AS measure_id 
UNION ALL
SELECT 'HBIPS-4' AS measure_id 
UNION ALL
SELECT 'HBIPS-5' AS measure_id 
UNION ALL
SELECT 'HBIPS-6' AS measure_id 
UNION ALL
SELECT 'HBIPS-7' AS measure_id
) m1 LEFT JOIN measure_date md
ON UPPER(CASE REGEXP_EXTRACT(m1.measure_id, '((MORT|READM)+.*|([A-Z]|_)+[0-9]+[^_]*)(.*)', 1) 
WHEN '' THEN m1.measure_id ELSE REGEXP_EXTRACT(m1.measure_id, '((MORT|READM)+.*|([A-Z]|_)+[0-9]+[^_]*)(.*)', 1) END) = UPPER(md.measure_id)) m2;