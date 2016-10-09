----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: best_hospitals.sql
-- Description: SQL script to rank hospitals by calculating an adjusted 
--              average score across all measures applicable to spercific
--              procedures.  A temp table named "measure_count_temp" 
--              will be created to help with the select query.
-- Date       : 10/8/2016
----------------------------------------------------------------------------
DROP TABLE measure_count_temp;

CREATE TEMPORARY TABLE measure_count_temp AS
SELECT COUNT(DISTINCT measure_id) AS total_measure_count 
FROM measure_score;

SELECT
msn.provider_id, 
h.hospital_name, 
h.state, 
msn.measure_count, 
msn.total_score, 
msn.avg_score AS straight_avg_score, 
mct.total_measure_count, 
msn.total_score / mct.total_measure_count AS adjusted_avg_score
FROM 
(SELECT provider_id, 
COUNT(measure_id) AS measure_count, 
SUM(normalized_score) AS total_score, 
AVG(normalized_score) AS avg_score
FROM measure_score_normalized
WHERE normalized_score IS NOT NULL
GROUP BY provider_id) msn
LEFT JOIN hospital h ON msn.provider_id = h.provider_id
INNER JOIN measure_count_temp mct
ORDER BY adjusted_avg_score DESC
LIMIT 10;