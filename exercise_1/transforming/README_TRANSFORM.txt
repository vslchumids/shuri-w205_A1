W205 Section 5
Exercise 1 Final Submission 
README_TRANSFORM
Vincent Chu
10/9/2016

I. Transformation Scripts Execution Order
==========================================
Some of my create table scripts for the transformation phase (i.e., in the transforming folder) have 
dependencies on other scripts, please run these scripts in the following order:

1) create_hospital.sql
2) create_procedure.sql
3) create_measure.sql
4) create_measure_score.sql
5) create_measure_score_summary.sql
6) create_measure_score_normalized.sql
7) create_survey_response.sql
8) create_table_indexes.sql

