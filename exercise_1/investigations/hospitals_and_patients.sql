----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: hospitals_and_patients.sql
-- Description: SQL script to calculate Pearson's Correlation value between
--              a hospital's adjusted average score and its average score 
--              from patient surveys.  A temp table named "measure_count_temp" 
--              will be created to help with the select query.
-- Date       : 10/8/2016
----------------------------------------------------------------------------
DROP TABLE measure_count_temp;

CREATE TEMPORARY TABLE measure_count_temp AS
SELECT COUNT(DISTINCT measure_id) AS total_measure_count 
FROM measure_score;

DROP TABLE hospital_score_temp;

CREATE TEMPORARY TABLE hospital_score_temp AS
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
ORDER BY adjusted_avg_score DESC;

SELECT corr(hst.adjusted_avg_score, (sr.base_score + sr.consistency_score)) AS hospital_patient_corr
FROM hospital_score_temp hst INNER JOIN survey_response sr
ON hst.provider_id = sr.provider_id
WHERE hst.adjusted_avg_score IS NOT NULL 
AND sr.base_score IS NOT NULL
AND sr.consistency_score IS NOT NULL;

SELECT corr((sr.base_score + sr.consistency_score), hst.adjusted_avg_score) AS hospital_patient_corr
FROM hospital_score_temp hst INNER JOIN survey_response sr
ON hst.provider_id = sr.provider_id
WHERE hst.adjusted_avg_score IS NOT NULL 
AND sr.base_score IS NOT NULL
AND sr.consistency_score IS NOT NULL;
