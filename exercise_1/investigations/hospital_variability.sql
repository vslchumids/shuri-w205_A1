----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: hospital_variability.sql
-- Description: SQL script to rank procedures and measures based on 
--              variability in quality between hospitals (i.e., standard 
--              deviation of the hospitals' adjusted average scores). 
-- Date       : 10/9/2016
----------------------------------------------------------------------------
-- By procedure
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
 
-- By measure
SELECT mss.measure_id, 
       m.measure_name, 
       m.procedure_id, 
       mss.score_var, 
       mss.score_stddev
FROM measure_score_summary mss LEFT JOIN measure m
ON mss.measure_id = m.measure_id
ORDER BY score_stddev DESC
LIMIT 10;