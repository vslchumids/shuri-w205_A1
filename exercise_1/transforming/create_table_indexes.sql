----------------------------------------------------------------------------
-- W205 Section 5 Exercise 1 Final Submission
-- Vincent Chu
-- File name: create_table_indexes.sql
-- Description: SQL script to create indexes for the "transformation" tables
--              to make queries for data investigation more efficient.
-- Date       : 10/8/2016
----------------------------------------------------------------------------

CREATE INDEX index_hospital_1 ON TABLE hospital(provider_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_measure_1 ON TABLE measure(measure_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_procedure_1 ON TABLE procedure(procedure_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_measure_score_1 ON TABLE measure_score(provider_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_measure_score_2 ON TABLE measure_score(measure_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_measure_score_summary_1 ON TABLE measure_score_summary(measure_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_measure_score_normalized_1 ON TABLE measure_score_normalized(provider_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_measure_score_normalized_2 ON TABLE measure_score_normalized(measure_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_survey_category_1 ON TABLE survey_category(survey_category_code) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
CREATE INDEX index_survey_response_1 ON TABLE survey_response(provider_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;