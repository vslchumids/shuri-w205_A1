----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: hive_base_ddl.sql
-- Description: SQL script to create tables for the base files loaded 
--              into HDFS with load_data_lake.sh
-- Date       : 10/7/2016
----------------------------------------------------------------------------

----------------------------------------------------------------------------
-- Create table for hospital objects from "Hospital General Information.csv"
----------------------------------------------------------------------------
DROP TABLE hospital_info;

CREATE EXTERNAL TABLE IF NOT EXISTS hospital_info
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
hospital_type string,
hospital_ownership string,
emergency_services string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';

----------------------------------------------------------------------------
-- Create table for measure objects from "Measure Dates.csv"
----------------------------------------------------------------------------
DROP TABLE measure_date;

CREATE EXTERNAL TABLE IF NOT EXISTS measure_date
(measure_name string,
measure_id string,
measure_start_quarter string,
measure_start_date string,
measure_end_quarter string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/measures';

----------------------------------------------------------------------------
-- Create table for hospital structural measure objects from 
-- "Structural Measures - Hospital.csv"
----------------------------------------------------------------------------
DROP TABLE structural_measure;

CREATE EXTERNAL TABLE IF NOT EXISTS structural_measure
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
measure_name string,
measure_id string,
measure_response string,
footnote string,
measure_start_date string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/structural_measures';

----------------------------------------------------------------------------
-- Create table for patient survey response objects from 
-- "hvbp_hcahps_05_28_2015.csv"
----------------------------------------------------------------------------
DROP TABLE survey_res;

CREATE EXTERNAL TABLE IF NOT EXISTS survey_res
(provider_number string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
communication_with_nurses_achievement_points string,
communication_with_nurses_improvement_points string,
communication_with_nurses_dimension_score string,
communication_with_doctors_achievement_points string,
communication_with_doctors_improvement_points string,
communication_with_doctors_dimension_score string,
responsiveness_of_hospital_staff_achievement_points string,
responsiveness_of_hospital_staff_improvement_points string,
responsiveness_of_hospital_staff_dimension_score string,
pain_management_achievement_points string,
pain_management_improvement_points string,
pain_management_dimension_score string,
communication_about_medicines_achievement_points string,
communication_about_medicines_improvement_points string,
communication_about_medicines_dimension_score string,
cleanliness_and_quietness_of_hospital_environment_achievement_points string,
cleanliness_and_quietness_of_hospital_environment_improvement_points string,
cleanliness_and_quietness_of_hospital_environment_dimension_score string,
discharge_information_achievement_points string,
discharge_information_improvement_points string,
discharge_information_dimension_score string,
overall_rating_of_hospital_achievement_points string,
overall_rating_of_hospital_improvement_points string,
overall_rating_of_hospital_dimension_score string,
hcahps_base_score string,
hcahps_consistency_score string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/surveys';

----------------------------------------------------------------------------
-- Create table for complication objects from 
-- "Complications - Hospital.csv"
----------------------------------------------------------------------------
DROP TABLE complication;

CREATE EXTERNAL TABLE IF NOT EXISTS complication
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
measure_name string,
measure_id string,
compared_to_national string,
denominator string,
score string,
lower_estimate string,
higher_estimate string,
footnote string,
measure_start_date string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/complications';

----------------------------------------------------------------------------
-- Create table for infection (HAI) objects from 
-- "Healthcare Associated Infections - Hospital.csv"
----------------------------------------------------------------------------
DROP TABLE infection;

CREATE EXTERNAL TABLE IF NOT EXISTS infection
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
measure_name string,
measure_id string,
compared_to_national string,
score string,
footnote string,
measure_start_date string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/infections';

----------------------------------------------------------------------------
-- Create table for outpatient imaging efficiency (OIE) objects from 
-- "Outpatient Imaging Efficiency - Hospital.csv"
----------------------------------------------------------------------------
DROP TABLE outpatient_imaging;

CREATE EXTERNAL TABLE IF NOT EXISTS outpatient_imaging
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
measure_id string,
measure_name string,
score string,
footnote string,
measure_start_date string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/outpatient_imaging';

----------------------------------------------------------------------------
-- Create table for readmission and death objects from 
-- "Readmissions and Deaths - Hospital.csv"
----------------------------------------------------------------------------
DROP TABLE readmission_death;

CREATE EXTERNAL TABLE IF NOT EXISTS readmission_death
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
measure_name string,
measure_id string,
compared_to_national string,
denominator string,
score string,
lower_estimate string,
higher_estimate string,
footnote string,
measure_start_date string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/readmissions_deaths';

----------------------------------------------------------------------------
-- Create table for timely and effective care objects from 
-- "Timely and Effective Care - Hospital.csv"
----------------------------------------------------------------------------
DROP TABLE effective_care;

CREATE EXTERNAL TABLE IF NOT EXISTS effective_care
(provider_id string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
phone_number string,
condition string,
measure_id string,
measure_name string,
score string,
sample string,
footnote string,
measure_start_date string,
measure_end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care';

----------------------------------------------------------------------------
-- Create table for impatient psychiatric facility quality (IPFQR) objects 
-- from "HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv"
----------------------------------------------------------------------------
DROP TABLE inpatient_psychiatric;

CREATE EXTERNAL TABLE IF NOT EXISTS inpatient_psychiatric
(provider_number string,
hospital_name string,
address string,
city string,
state string,
zip_code string,
county_name string,
hbips_2_measure_description string,
hbips_2_overall_rate_per_1000 string,
hbips_2_overall_num string,
hbips_2_overall_den string,
hbips_2_overall_footnote string,
hbips_2_1_12_rate_per_1000 string,
hbips_2_1_12_num string,
hbips_2_1_12_den string,
hbips_2_1_12_footnote string,
hbips_2_13_17_rate_per_1000 string,
hbips_2_13_17_num string,
hbips_2_13_17_den string,
hbips_2_13_17_footnote string,
hbips_2_18_64_rate_per_1000 string,
hbips_2_18_64_num string,
hbips_2_18_64_den string,
hbips_2_18_64_footnote string,
hbips_2_65_over_rate_per_1000 string,
hbips_2_65_over_num string,
hbips_2_65_over_den string,
hbips_2_65_over_footnote string,
hbips_3_measure_description string,
hbips_3_overall_rate_per_1000 string,
hbips_3_overall_num string,
hbips_3_overall_den string,
hbips_3_overall_footnote string,
hbips_3_1_12_rate_per_1000 string,
hbips_3_1_12_num string,
hbips_3_1_12_den string,
hbips_3_1_12_footnote string,
hbips_3_13_17_rate_per_1000 string,
hbips_3_13_17_num string,
hbips_3_13_17_den string,
hbips_3_13_17_footnote string,
hbips_3_18_64_rate_per_1000 string,
hbips_3_18_64_num string,
hbips_3_18_64_den string,
hbips_3_18_64_footnote string,
hbips_3_65_over_rate_per_1000 string,
hbips_3_65_over_num string,
hbips_3_65_over_den string,
hbips_3_65_over_footnote string,
hbips_4_measure_description string,
hbips_4_overall_pct_of_total string,
hbips_4_overall_num string,
hbips_4_overall_den string,
hbips_4_overall_footnote string,
hbips_4_1_12_pct_of_total string,
hbips_4_1_12_num string,
hbips_4_1_12_den string,
hbips_4_1_12_footnote string,
hbips_4_13_17_pct_of_total string,
hbips_4_13_17_num string,
hbips_4_13_17_den string,
hbips_4_13_17_footnote string,
hbips_4_18_64_pct_of_total string,
hbips_4_18_64_num string,
hbips_4_18_64_den string,
hbips_4_18_64_footnote string,
hbips_4_65_over_pct_of_total string,
hbips_4_65_over_num string,
hbips_4_65_over_den string,
hbips_4_65_over_footnote string,
hbips_5_measure_description string,
hbips_5_overall_pct_of_total string,
hbips_5_overall_num string,
hbips_5_overall_den string,
hbips_5_overall_footnote string,
hbips_5_1_12_pct_of_total string,
hbips_5_1_12_num string,
hbips_5_1_12_den string,
hbips_5_1_12_footnote string,
hbips_5_13_17_pct_of_total string,
hbips_5_13_17_num string,
hbips_5_13_17_den string,
hbips_5_13_17_footnote string,
hbips_5_18_64_pct_of_total string,
hbips_5_18_64_num string,
hbips_5_18_64_den string,
hbips_5_18_64_footnote string,
hbips_5_65_over_pct_of_total string,
hbips_5_65_over_num string,
hbips_5_65_over_den string,
hbips_5_65_over_footnote string,
hbips_6_measure_description string,
hbips_6_overall_pct_of_total string,
hbips_6_overall_num string,
hbips_6_overall_den string,
hbips_6_overall_footnote string,
hbips_6_1_12_pct_of_total string,
hbips_6_1_12_num string,
hbips_6_1_12_den string,
hbips_6_1_12_footnote string,
hbips_6_13_17_pct_of_total string,
hbips_6_13_17_num string,
hbips_6_13_17_den string,
hbips_6_13_17_footnote string,
hbips_6_18_64_pct_of_total string,
hbips_6_18_64_num string,
hbips_6_18_64_den string,
hbips_6_18_64_footnote string,
hbips_6_65_over_pct_of_total string,
hbips_6_65_over_num string,
hbips_6_65_over_den string,
hbips_6_65_over_footnote string,
hbips_7_measure_description string,
hbips_7_overall_pct_of_total string,
hbips_7_overall_num string,
hbips_7_overall_den string,
hbips_7_overall_footnote string,
hbips_7_1_12_pct_of_total string,
hbips_7_1_12_num string,
hbips_7_1_12_den string,
hbips_7_1_12_footnote string,
hbips_7_13_17_pct_of_total string,
hbips_7_13_17_num string,
hbips_7_13_17_den string,
hbips_7_13_17_footnote string,
hbips_7_18_64_pct_of_total string,
hbips_7_18_64_num string,
hbips_7_18_64_den string,
hbips_7_18_64_footnote string,
hbips_7_65_over_pct_of_total string,
hbips_7_65_over_num string,
hbips_7_65_over_den string,
hbips_7_65_over_footnote string,
start_date string,
end_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/inpatient_psychiatric';

----------------------------------------------------------------------------
-- Create table for cancer hospital quality (PCHQR) objects from
-- "HOSPITAL_QUARTERLY_QUALITYMEASURE_PCH_HOSPITAL.csv"
----------------------------------------------------------------------------
DROP TABLE cancer;

CREATE EXTERNAL TABLE IF NOT EXISTS cancer
(provider_id string,
hospital_name string,
hospital_type string,
address string,
city string,
state string,
zip_code string,
county_name string,
measure_id string,
measure_description string,
numerator string,
denominator string,
footnote string,
rptg_prd_start_dt string,
rptg_prd_end_dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/cancer';