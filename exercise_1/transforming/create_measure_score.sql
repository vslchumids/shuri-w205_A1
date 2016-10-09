----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_measure_score.sql
-- Description: SQL script to create the measure_score table by transforming 
--              data from the various raw score tables, including infection, 
--              complication, outpatient_imaging, readmission_death, 
--              effective_care, cancer and inpatient_psychiatric.
-- Date       : 10/7/2016
----------------------------------------------------------------------------
DROP TABLE measure_score;

CREATE TABLE measure_score AS
SELECT DISTINCT provider_id, measure_id, score
FROM (SELECT provider_id,
measure_id,
CASE score 
WHEN 'Not Available' THEN NULL
ELSE cast(score AS DOUBLE) END AS score
FROM infection
WHERE measure_id LIKE 'HAI_%_SIR'
UNION ALL
SELECT provider_id,
measure_id,
CASE score 
WHEN 'Not Available' THEN NULL
ELSE cast(score AS DOUBLE) END AS score
FROM complication
WHERE measure_id IN ('PSI_4_SURG_COMP', 'PSI_90_SAFETY', 'COMP_HIP_KNEE')
UNION ALL
SELECT provider_id,
measure_id,
CASE score 
WHEN 'Not Available' THEN NULL
ELSE cast(score AS DOUBLE) END AS score
FROM outpatient_imaging
UNION ALL
SELECT provider_id,
measure_id,
CASE score 
WHEN 'Not Available' THEN NULL
ELSE cast(score AS DOUBLE) END AS score
FROM readmission_death
UNION ALL
SELECT provider_id,
measure_id,
CASE score 
WHEN 'Not Available' THEN NULL
ELSE cast(score AS DOUBLE) END AS score
FROM effective_care
WHERE measure_id <> 'EDV'
UNION ALL
SELECT provider_id,
measure_id,
IF(numerator <> 'Not Available' AND denominator <> 'Not Available' AND cast(denominator AS DOUBLE) <> 0, 
cast(numerator AS DOUBLE) / cast(denominator AS DOUBLE), NULL) AS score
FROM cancer
UNION ALL
SELECT provider_number AS provider_id, 
'HBIPS-2' AS measure_id, 
CASE hbips_2_overall_rate_per_1000
WHEN 'Not Available' THEN NULL
ELSE cast(hbips_2_overall_rate_per_1000 AS DOUBLE) END AS score
FROM inpatient_psychiatric
UNION ALL
SELECT provider_number AS provider_id, 
'HBIPS-3' AS measure_id, 
CASE hbips_3_overall_rate_per_1000
WHEN 'Not Available' THEN NULL
ELSE cast(hbips_3_overall_rate_per_1000 AS DOUBLE) END AS score
FROM inpatient_psychiatric
UNION ALL
SELECT provider_number AS provider_id, 
'HBIPS-4' AS measure_id, 
CASE hbips_4_overall_pct_of_total
WHEN 'Not Available' THEN NULL
ELSE cast(hbips_4_overall_pct_of_total AS DOUBLE) END AS score
FROM inpatient_psychiatric
UNION ALL
SELECT provider_number AS provider_id, 
'HBIPS-5' AS measure_id, 
CASE hbips_5_overall_pct_of_total
WHEN 'Not Available' THEN NULL
ELSE cast(hbips_5_overall_pct_of_total AS DOUBLE) END AS score
FROM inpatient_psychiatric
UNION ALL
SELECT provider_number AS provider_id, 
'HBIPS-6' AS measure_id, 
CASE hbips_6_overall_pct_of_total
WHEN 'Not Available' THEN NULL
ELSE cast(hbips_6_overall_pct_of_total AS DOUBLE) END AS score
FROM inpatient_psychiatric
UNION ALL
SELECT provider_number AS provider_id, 
'HBIPS-7' AS measure_id, 
CASE hbips_7_overall_pct_of_total
WHEN 'Not Available' THEN NULL
ELSE cast(hbips_7_overall_pct_of_total AS DOUBLE) END AS score
FROM inpatient_psychiatric
) ms;