----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: hospital_variability.sql
-- Description: SQL script to rank procedures and measures based on 
--              variability in quality between hospitals (i.e., standard 
--              deviation of the hospitals' adjusted average scores). 
-- Date       : 10/9/2016
----------------------------------------------------------------------------
-- Select the top 10 procedures with the highest standard deviation 
-- (variability) across all hospitals
SELECT m.procedure_id, 
       p.procedure_name, 
       AVG(mss.score_var) AS score_var, 
       AVG(mss.score_stddev) AS score_stddev
FROM measure_score_summary mss LEFT JOIN measure m
ON mss.measure_id = m.measure_id
LEFT JOIN procedure p
ON m.procedure_id = p.procedure_id
GROUP BY m.procedure_id, p.procedure_name
ORDER BY score_stddev DESC
LIMIT 10;
 
-- Select the top 10 procedures with the highest standard deviation 
-- (variability) across all hospitals, filtering out all measures
-- with data on < 50% of the entire population of hospitals, i.e., 
-- all hospitals with any data for any of the measure quality data 
-- collected in this database.
SELECT m.procedure_id, 
       p.procedure_name, 
       AVG(mss.score_var) AS score_var, 
       AVG(mss.score_stddev) AS score_stddev
FROM measure_score_summary mss LEFT JOIN measure m
ON mss.measure_id = m.measure_id
LEFT JOIN procedure p
ON m.procedure_id = p.procedure_id
WHERE mss.pct_hospitals_w_score >= 0.5
GROUP BY m.procedure_id, p.procedure_name
ORDER BY score_stddev DESC
LIMIT 10;