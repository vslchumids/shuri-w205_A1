############################################################################
# W205 Section 5 Exercise 1 Final Submission
# Vincent Chu
# File name  : load_data_lake.sh
# Description: sh script to unzip source file for Hospital Compare data 
#              and load selected raw data files to HDFS Data Lake
# Date       : 10/7/2016
############################################################################
############################################################################
# Setting up directories in local
############################################################################

# Display current directory in local
pwd

# Recursively remove the zip directory in local, if it exists
rm -rf zip
# Re-create the zip directory, which will hold the zip file with flat data files for Hospital Compare
mkdir zip

# Recursively remove the csv directory in local, if it exists
rm -rf csv
# Re-create the csv directory, which will hold the raw csv files for Hospital Compare
mkdir csv

# Recursively remove the load_csv directory in local, if it exists
rm -rf load_csv
# Re-create the load_csv directory, which will hold the renamed, no-header version of the csv data files
mkdir load_csv

# Change directory to zip in local
cd zip

# The zip file with flat data files for Hospital Compare
wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary\&filename=Hospital_Revised_Flatfiles.zip

# Rename the downloaded file to the actual name of the source file
mv "Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip; charset=binary&filename=Hospital_Revised_Flatfiles.zip" Hospital_Revised_Flatfiles.zip

# Change back to parent directory in local
cd ..

# Unzip the contents of Hospital_Revised_Flatfiles.zip to the csv subdirectory in local
unzip zip/Hospital_Revised_Flatfiles.zip -d csv/

# Change directory to load_csv in local
cd load_csv

############################################################################
# Setting up directories in hdfs
############################################################################

# Recursively remove the hospital_compare directory in HDFS, if it exists
hdfs dfs -rm -r /user/w205/hospital_compare

# Create a new directory in HDFS for Exercise 1: Hospital Compare
hdfs dfs -mkdir /user/w205/hospital_compare

############################################################################
# Load raw data file for hospitals
############################################################################

# Make a copy of "Hospital General Information.csv", remove the header 
# row, rename it to hospitals and place it in the load_csv subdirectory
tail -n +2 ../csv/Hospital\ General\ Information.csv > hospitals.csv

# Create a new subdirectory under /hospital_compare for hospitals.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals

# Load the hospitals.csv file to HDFS under hospital_compare/hospitals
hdfs dfs -put hospitals.csv /user/w205/hospital_compare/hospitals

############################################################################
# Load raw data file for measures
############################################################################

# Make a copy of "Measure Dates.csv", remove the header 
# row, rename it to measures and place it in the load_csv subdirectory
tail -n +2 ../csv/Measure\ Dates.csv > measures.csv

# Create a new subdirectory under /hospital_compare for measures.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/measures

# Load the measures.csv file to HDFS under hospital_compare/measures
hdfs dfs -put measures.csv /user/w205/hospital_compare/measures

############################################################################
# Load raw data file for structural measures
############################################################################

# Make a copy of "Structural Measures - Hospital.csv", remove the header 
# row, rename it to structural_measures and place it in the load_csv subdirectory
tail -n +2 ../csv/Structural\ Measures\ -\ Hospital.csv > structural_measures.csv

# Create a new subdirectory under /hospital_compare for 
# structural_measures.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/structural_measures

# Load the structural_measures.csv file to HDFS under hospital_compare/structural_measures
hdfs dfs -put structural_measures.csv /user/w205/hospital_compare/structural_measures

############################################################################
# Load raw data file for survey responses
############################################################################

# Make a copy of "hvbp_hcahps_05_28_2015.csv", remove the header 
# row, rename it to survey_responses and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_hcahps_05_28_2015.csv > survey_responses.csv

# Create a new subdirectory under /hospital_compare for 
# survey_responses.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/surveys

# Load the survey_responses.csv file to HDFS under hospital_compare/surveys
hdfs dfs -put survey_responses.csv /user/w205/hospital_compare/surveys

############################################################################
# Load raw data file for complications
############################################################################

# Make a copy of "Complications - Hospital.csv", remove the header 
# row, rename it to complications and place it in the load_csv subdirectory
tail -n +2 ../csv/Complications\ -\ Hospital.csv > complications.csv

# Create a new subdirectory under /hospital_compare for complications.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/complications

# Load the complications.csv file to HDFS under hospital_compare/complications
hdfs dfs -put complications.csv /user/w205/hospital_compare/complications

############################################################################
# Load raw data file for infections (HAIs)
############################################################################

# Make a copy of "Healthcare Associated Infections - Hospital.csv", remove 
# the header row, rename it to infections and place it in the load_csv 
# subdirectory
tail -n +2 ../csv/Healthcare\ Associated\ Infections\ -\ Hospital.csv > infections.csv

# Create a new subdirectory under /hospital_compare for infections.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/infections

# Load the infections.csv file to HDFS under hospital_compare/infections
hdfs dfs -put infections.csv /user/w205/hospital_compare/infections

############################################################################
# Load raw data file for outpatient imaging efficiency (OIE)
############################################################################

# Make a copy of "Outpatient Imaging Efficiency - Hospital.csv", remove 
# the header row, rename it to outpatient_imaging and place it in the 
# load_csv subdirectory
tail -n +2 ../csv/Outpatient\ Imaging\ Efficiency\ -\ Hospital.csv > outpatient_imaging.csv

# Create a new subdirectory under /hospital_compare for outpatient_imaging.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/outpatient_imaging

# Load the outpatient_imaging.csv file to HDFS under hospital_compare/outpatient_imaging
hdfs dfs -put outpatient_imaging.csv /user/w205/hospital_compare/outpatient_imaging

############################################################################
# Load raw data file for readmissions and deaths
############################################################################

# Make a copy of "Readmissions and Deaths - Hospital.csv", remove 
# the header row, rename it to readmissions_deaths and place it in the load_csv 
# subdirectory
tail -n +2 ../csv/Readmissions\ and\ Deaths\ -\ Hospital.csv > readmissions_deaths.csv

# Create a new subdirectory under /hospital_compare for readmissions_deaths.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions_deaths

# Load the readmissions_deaths.csv file to HDFS under hospital_compare/readmissions_deaths
hdfs dfs -put readmissions_deaths.csv /user/w205/hospital_compare/readmissions_deaths

############################################################################
# Load raw data file for timely and effective care
############################################################################

# Make a copy of "Timely and Effective Care - Hospital.csv", remove 
# the header row, rename it to effective_care and place it in the load_csv 
# subdirectory
tail -n +2 ../csv/Timely\ and\ Effective\ Care\ -\ Hospital.csv > effective_care.csv

# Create a new subdirectory under /hospital_compare for effective_care.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care

# Load the effective_care.csv file to HDFS under hospital_compare/effective_care
hdfs dfs -put effective_care.csv /user/w205/hospital_compare/effective_care

############################################################################
# Load raw data file for inpatient psychiatric facility quality (IPFQR)
############################################################################

# Make a copy of "HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv", remove 
# the header row, rename it to inpatient_psychiatric and place it in the load_csv subdirectory
tail -n +2 ../csv/HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv > inpatient_psychiatric.csv

# Create a new subdirectory under /hospital_compare for inpatient_psychiatric.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/inpatient_psychiatric

# Load the inpatient_psychiatric.csv file to HDFS under hospital_compare/inpatient_psychiatric
hdfs dfs -put inpatient_psychiatric.csv /user/w205/hospital_compare/inpatient_psychiatric

############################################################################
# Load raw data file for cancer hospital quality (PCHQR)
############################################################################

# Make a copy of "HOSPITAL_QUARTERLY_QUALITYMEASURE_PCH_HOSPITAL.csv", remove 
# the header row, rename it to cancer and place it in the load_csv subdirectory
tail -n +2 ../csv/HOSPITAL_QUARTERLY_QUALITYMEASURE_PCH_HOSPITAL.csv > cancer.csv

# Create a new subdirectory under /hospital_compare for cancer.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/cancer

# Load the cancer.csv file to HDFS under hospital_compare/cancer
hdfs dfs -put cancer.csv /user/w205/hospital_compare/cancer

############################################################################
# Load raw data file for the list of measures used in Hospital Value-Based 
# Purchasing (HVBP) quality to payment scores
############################################################################

# Make a copy of "hvbp_quarters.csv", remove the header row, rename it to 
# hvbp_quality_measures and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_quarters.csv > hvbp_quality_measures.csv

# Create a new subdirectory under /hospital_compare for hvbp_quality_measures.csv 
# in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_quality_measures

# Load the hvbp_quality_measures.csv file to HDFS under hospital_compare/hvbp_quality_measures
hdfs dfs -put hvbp_quality_measures.csv /user/w205/hospital_compare/hvbp_quality_measures

############################################################################
# Load raw data file for Hospital Value-Based Purchasing (HVBP) quality 
# to payment scores for Pneumonia (PN)
############################################################################

# Make a copy of "hvbp_pn_05_28_2015.csv", remove the header row, rename 
# it to hvbp_pn and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_pn_05_28_2015.csv > hvbp_pn.csv

# Create a new subdirectory under /hospital_compare for hvbp_pn.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_pn

# Load the hvbp_pn.csv file to HDFS under hospital_compare/hvbp_pn
hdfs dfs -put hvbp_pn.csv /user/w205/hospital_compare/hvbp_pn

############################################################################
# Load raw data file for Hospital Value-Based Purchasing (HVBP) quality 
# to payment scores for Surgical Care Improvement Program (SCIP)
############################################################################

# Make a copy of "hvbp_scip_05_28_2015.csv", remove the header row, rename 
# it to hvbp_scip and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_scip_05_28_2015.csv > hvbp_scip.csv

# Create a new subdirectory under /hospital_compare for hvbp_scip.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_scip

# Load the hvbp_scip.csv file to HDFS under hospital_compare/hvbp_scip
hdfs dfs -put hvbp_scip.csv /user/w205/hospital_compare/hvbp_scip

############################################################################
# Load raw data file for Hospital Value-Based Purchasing (HVBP) quality 
# to payment scores for Acute Myocardial Infarction (AMI)
############################################################################

# Make a copy of "hvbp_ami_05_28_2015.csv", remove the header row, rename 
# it to hvbp_ami and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_ami_05_28_2015.csv > hvbp_ami.csv

# Create a new subdirectory under /hospital_compare for hvbp_ami.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_ami

# Load the hvbp_ami.csv file to HDFS under hospital_compare/hvbp_ami
hdfs dfs -put hvbp_ami.csv /user/w205/hospital_compare/hvbp_ami

############################################################################
# Load raw data file for Hospital Value-Based Purchasing (HVBP) quality 
# to payment scores for Healthcare-Associated Infections (HAI)
############################################################################

# Make a copy of "hvbp_hai_05_28_2015.csv", remove the header row, rename 
# it to hvbp_hai and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_hai_05_28_2015.csv > hvbp_hai.csv

# Create a new subdirectory under /hospital_compare for hvbp_hai.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_hai

# Load the hvbp_hai.csv file to HDFS under hospital_compare/hvbp_hai
hdfs dfs -put hvbp_hai.csv /user/w205/hospital_compare/hvbp_hai

############################################################################
# Load raw data file for Hospital Value-Based Purchasing (HVBP) quality 
# to payment scores for Heart Failures (HF)
############################################################################

# Make a copy of "hvbp_hf_05_28_2015.csv", remove the header row, rename 
# it to hvbp_hf and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_hf_05_28_2015.csv > hvbp_hf.csv

# Create a new subdirectory under /hospital_compare for hvbp_hf.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_hf

# Load the hvbp_hf.csv file to HDFS under hospital_compare/hvbp_hf
hdfs dfs -put hvbp_hf.csv /user/w205/hospital_compare/hvbp_hf

############################################################################
# Load raw data file for Hospital Value-Based Purchasing (HVBP) quality 
# to payment scores for outcomes of complications and HAIs
############################################################################

# Make a copy of "hvbp_outcome_05_18_2015.csv", remove the header row, 
# rename it to hvbp_outcomes and place it in the load_csv subdirectory
tail -n +2 ../csv/hvbp_outcome_05_18_2015.csv > hvbp_outcomes.csv

# Create a new subdirectory under /hospital_compare for hvbp_outcomes.csv in HDFS
hdfs dfs -mkdir /user/w205/hospital_compare/hvbp_outcomes

# Load the hvbp_outcomes.csv file to HDFS under hospital_compare/hvbp_outcomes
hdfs dfs -put hvbp_outcomes.csv /user/w205/hospital_compare/hvbp_outcomes