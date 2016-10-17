----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_measure_score_summary.sql
-- Description: SQL script to create the measure_score_summary table which 
--              stores the aggregate scores by measure_id across all 
--              hospitals.  A temp table named "hospital_with_measure_temp" 
--              will be created to help with creating the 
--              measure_score_summary table.
-- Date       : 10/8/2016
----------------------------------------------------------------------------

DROP TABLE hospital_with_measure_temp;

CREATE TEMPORARY TABLE hospital_with_measure_temp AS
SELECT COUNT(DISTINCT provider_id) AS hospital_count 
FROM measure_score;

DROP TABLE measure_score_summary;

CREATE TABLE measure_score_summary AS
SELECT measure_id, 
       min_score, 
       max_score, 
       total_score, 
       score_count,
       score_count / hospital_count AS pct_hospitals_w_score,  
       avg_score, 
       score_var, 
       score_stddev, 
       hospital_count
FROM
(SELECT measure_id, 
       MIN(score) AS min_score, 
       MAX(score) AS max_score, 
       SUM(score) AS total_score, 
       COUNT(score) AS score_count, 
       AVG(score) AS avg_score, 
       VAR_POP(score) AS score_var, 
       STDDEV_POP(score) AS score_stddev
FROM measure_score
WHERE score IS NOT NULL
GROUP BY measure_id) ms
INNER JOIN 
hospital_with_measure_temp h;
