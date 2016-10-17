----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_survey_response.sql
-- Description: SQL Script to transform raw data table survey_res into 
--              the survey_response table for data investigation purposes.
-- Date       : 10/8/2016
----------------------------------------------------------------------------
DROP TABLE survey_response;

CREATE TABLE survey_response AS
SELECT DISTINCT provider_number AS provider_id, 
CAST(hcahps_base_score AS DOUBLE) AS base_score,
CAST(hcahps_consistency_score AS DOUBLE) AS consistency_score
FROM survey_res;