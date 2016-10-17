----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_measure_score_normalized.sql
-- Description: SQL script to create the measure_score_normalized table 
--              by "normalizing" data in the measure_score table.  All 
--              scores are normalized based on the min and max scores by 
--              measure_id on a 0 to 1 scale.  Due to the fact that the 
--              measures are on different "scoring systems", i.e., some 
--              favor a higher raw score and others favor a lower raw score, 
--              the semi-normalized scores for measures favoring lower raw 
--              scores will then be subtracted from 1 so that the lowest 
--              raw score will received a normalized score of 1.
-- Date       : 10/8/2016
----------------------------------------------------------------------------
DROP TABLE measure_score_normalized;

CREATE TABLE measure_score_normalized AS
SELECT ms.provider_id, 
       ms.measure_id, 
       CASE m.scoring_system 
       WHEN 'H'THEN (ms.score - mss.min_score) / (mss.max_score - mss.min_score) 
       WHEN 'L'THEN 1 - ((ms.score - mss.min_score) / (mss.max_score - mss.min_score))
       ELSE NULL END AS normalized_score
FROM measure_score ms LEFT JOIN measure m ON ms.measure_id = m.measure_id
                      LEFT JOIN measure_score_summary mss ON ms.measure_id = mss.measure_id;
