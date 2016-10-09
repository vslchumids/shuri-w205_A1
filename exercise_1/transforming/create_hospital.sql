----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_hospital.sql
-- Description: SQL Script to transform raw data table hospital_info into 
--              the hospital table for data investigation purposes.
-- Date       : 10/7/2016
----------------------------------------------------------------------------
DROP TABLE hospital;

CREATE TABLE hospital AS
SELECT DISTINCT provider_id,
                hospital_name,
                state
FROM hospital_info
ORDER BY state, hospital_name;