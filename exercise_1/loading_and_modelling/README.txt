W205 Section 5
Exercise 1 Part 1 - README
Vincent Chu

I. Raw Data File Selection
===========================
The first step I took was to select the raw files that are relevant for answering the following 4 questions:

1.	What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.
2.	What states are models of high-quality care? 
3.	Which procedures have the greatest variability between hospitals?
4.	Are average scores for hospital quality or procedural variability correlated with patient survey responses?

The selected raw data files were loaded as they are with no transformations for part 1.  Data transformations will be done in part 2 of this exercise.  
The following describes the raw data files that were excluded:

1. All national level data files, which are just rolled up from the hospital level data.  Also, the problem requires comparing quality and performance 
(from perspective of the patients) at the hospital and state level.
2. All state level files, which are just rolled up from the hospital level data.  Comparisons at the state level can be done by aggregate queries on 
top of hospital level data.
3. All payment files since our problems don¡¯t concern financial / economic efficiency.
4. All MSPB (Medicare Spending Per Beneficiary) files since our problems don¡¯t concern financial / economic efficiency.
5. The "HOSPITAL_QUARTERLY_HAC_DOMAIN_HOSPITAL.csv" file is excluded since scores of HAC (Hospital Acquired Conditions) are already captured as HAI 
and PSI measures in the Healthcare Associated Infections (HAI) and Complications files.
6. The "HCAHPS ¨C Hospital.csv" file is excluded since data from the HCAHPS survey is already captured in the "hvbp_hcahps_05_28_2015.csv" file, which 
is data used to link quality to payment in the Hospital Value-Based Purchasing (HVBP) Program.
7. The hvbp_Efficiency_05_20_2015.csv file was excluded since it is related to the MSPB measures, which measure financial / economic efficiency.
8. All files with the prefix ¡°FY2013_¡± since these are all payment related summary files.

II. Defining Procedures
========================
Since not all measures are directly tied to medical procedures, I grouped the measures found in the raw data files I chose to load into the HDFS data 
lake by their acronym prefix and selected only those that are relevant to procedures.  These acronym prefixes that grouped the measures would become 
my ¡°Procedure¡± entity, which refers to one of the following:

i) Medical procedures, e.g., HIP-KNEE (Total Hip/Knee Arthoplasty), etc.
ii) Medical conditions that will be treated by specific medical procedures, e.g., AMI (Acute Myocardial Infarction, aka Heart Attack), etc.
iii) General groups of measures related to an array of medical procedures , e.g., PSI (Patient Safety Indicators), etc.

Procedure	Procedure Description
---------	---------------------
AMI		Acute Myocardial Infarction
CAC		Children¡¯s Asthma Care
COMP_HIP_KNEE	Hip/knee replacement complications
ED		Emergency Department
HAI		Healthcare-Associated Infections
HBIPS		Hospital-Based Inpatient Psychiatric Services
HF		Heart Failure
IMM		Immunization
MORT		Mortality
OP		Outpatient
PCH		PPS-Exempt Cancer Hospital
PN		Pneumonia
PSI		Patient Safety Indicators
READM		Readmissions
SCIP		Surgical Care Improvement Project
STK		Stroke
VTE		Venous Thromboembolism

III. Measures vs. HVBP Quality Scores
======================================

While hospital-level measure data sets that contain scores for each of the (hospital, measure) combinations represented by the rows, the HVBP 
program generated very neat data sets with 4 metrics for the purpose of linking hospital quality to Medicare's payments: performance rate, 
achievement points, improvement points and measure score.  Although these metrics were not available for all measures, I included this data 
as a supplement and comparison to the hospital-level measure data.  I believe this data will be useful when we answer the procedure and 
quality related questions of this exercise.

IV. List of Tables Created
===========================
1. hospital - Contains provider ID, name and state of each hospitals in the data set.  

Source files: 
Hospital General Information.csv

2. survey_response - Contains the 3 types of scores/points in HVBP program's data of the Hospital Consumer Assessment of Healthcare Providers and Systems 
(HCAHPS) Patient Survey: achievement, improvement and dimension points.  The response_category field of this table will serve as an indicator of the survey 
questions corresponding to the points, for example, communication of the nurses, communication of the doctors, cleaniness of the hospitals, etc.  These 
information will be extracted from the column headings of the raw data.

Source files: 
hvbp_hcahps_05_28_2015.csv

3. procedure - Contains a list of categories whose measures will provide insights on quality and variation of medical procedures at the hospitals that are 
part of the data set.  Please refer to the section II above for the list of procedures and the way how the list was created.

Source files:
Complications - Hospital.csv
Healthcare Associated Infections - Hospital.csv
Outpatient Imaging Efficiency - Hospital.csv
Readmissions and Deaths - Hospital.csv
Timely and Effective Care - Hospital.csv
HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv
HOSPITAL_QUARTERLY_QUALITYMEASURE_PCH_HOSPITAL.csv
hvbp_ami_05_28_2015.csv
hvbp_hai_05_28_2015.csv
hvbp_hf_05_28_2015.csv
hvbp_outcome_05_18_2015.csv
hvbp_pn_05_28_2015.csv
hvbp_scip_05_28_2015.csv

4. measure - Contains the list of exhaustive of measures, including those missing in the "Measure Dates.csv" but appear in the individual 
measure data files.

Source files:
Measure Dates.csv
Structural Measures - Hospital.csv
Complications - Hospital.csv
Healthcare Associated Infections - Hospital.csv
Outpatient Imaging Efficiency - Hospital.csv
Readmissions and Deaths - Hospital.csv
Timely and Effective Care - Hospital.csv
HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv
HOSPITAL_QUARTERLY_QUALITYMEASURE_PCH_HOSPITAL.csv
hvbp_ami_05_28_2015.csv
hvbp_hai_05_28_2015.csv
hvbp_hf_05_28_2015.csv
hvbp_outcome_05_18_2015.csv
hvbp_pn_05_28_2015.csv
hvbp_scip_05_28_2015.csv
hvbp_quarters.csv

5. measure_score - Contains the measure id / score pairs for each hospital (where applicable and data is available).  Only the final score 
for each measure will be stored in this table.  In other words, all other values such as denominator/numerator used for calculations, 
high/low estimates, comparison to national average, etc. will be skipped. 

Source files: 
Complications - Hospital.csv
Healthcare Associated Infections - Hospital.csv
Outpatient Imaging Efficiency - Hospital.csv
Readmissions and Deaths - Hospital.csv
Timely and Effective Care - Hospital.csv
HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv
HOSPITAL_QUARTERLY_QUALITYMEASURE_PCH_HOSPITAL.csv

6. hvbp_quality_score - Contains the 4 main metrics in the HVBP data set: performance rate, achievement points, improvement points and 
measure score.  The corresponding measure_id will be extracted from the column headings of the hvbp raw data files.

Source files: 
hvbp_ami_05_28_2015.csv
hvbp_hai_05_28_2015.csv
hvbp_hf_05_28_2015.csv
hvbp_outcome_05_18_2015.csv
hvbp_pn_05_28_2015.csv
hvbp_scip_05_28_2015.csv
hvbp_quarters.csv