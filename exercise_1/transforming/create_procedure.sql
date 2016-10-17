----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_procedure.sql
-- Description: SQL Script to create the procedure table from metadata 
--              from the Data Dictionary
-- Date       : 10/7/2016
----------------------------------------------------------------------------
DROP TABLE procedure;

CREATE TABLE procedure AS
SELECT DISTINCT procedure_ID, procedure_name FROM
(SELECT 'AMI' AS procedure_ID, 'Acute Myocardial Infarction' AS procedure_name UNION ALL
SELECT 'CAC' AS procedure_ID, 'Children Asthma Care' AS procedure_name UNION ALL
SELECT 'COMP_HIP_KNEE' AS procedure_ID, 'Hip/knee replacement complications' AS procedure_name UNION ALL
SELECT 'ED' AS procedure_ID, 'Emergency Department' AS procedure_name UNION ALL
SELECT 'HAI' AS procedure_ID, 'Healthcare-Associated Infections' AS procedure_name UNION ALL
SELECT 'HBIPS' AS procedure_ID, 'Hospital-Based Inpatient Psychiatric Services' AS procedure_name UNION ALL
SELECT 'HF' AS procedure_ID, 'Heart Failure' AS procedure_name UNION ALL
SELECT 'IMM' AS procedure_ID, 'Immunization' AS procedure_name UNION ALL
SELECT 'MORT' AS procedure_ID, 'Mortality' AS procedure_name UNION ALL
SELECT 'OP' AS procedure_ID, 'Outpatient' AS procedure_name UNION ALL
SELECT 'PC' AS procedure_ID, 'New born Delivery Scheduling' AS procedure_name UNION ALL
SELECT 'PCH' AS procedure_ID, 'PPS-Exempt Cancer Hospital' AS procedure_name UNION ALL
SELECT 'PN' AS procedure_ID, 'Pneumonia' AS procedure_name UNION ALL
SELECT 'PSI' AS procedure_ID, 'Patient Safety Indicators' AS procedure_name UNION ALL
SELECT 'READM' AS procedure_ID, 'Readmissions' AS procedure_name UNION ALL
SELECT 'SCIP' AS procedure_ID, 'Surgical Care Improvement Project' AS procedure_name UNION ALL
SELECT 'STK' AS procedure_ID, 'Stroke' AS procedure_name UNION ALL
SELECT 'VTE' AS procedure_ID, 'Venous Thromboembolism' AS procedure_name) proc;