-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Persist DLT streaming view
-- MAGIC To easily support DLT / UC / ML during the preview with all cluster types, we temporary recopy the final DLT view to another UC table 

-- COMMAND ----------

CREATE OR REPLACE TABLE nhs_demos.nhs_england_ml_demo.drug_exposure_ml AS SELECT * FROM nhs_demos.nhs_england_ml_demo.drug_exposure;
CREATE OR REPLACE TABLE nhs_demos.nhs_england_ml_demo.person_ml AS SELECT * FROM nhs_demos.nhs_england_ml_demo.person;
CREATE OR REPLACE TABLE nhs_demos.nhs_england_ml_demo.patients_ml AS SELECT * FROM nhs_demos.nhs_england_ml_demo.patients;
CREATE OR REPLACE TABLE nhs_demos.nhs_england_ml_demo.encounters_ml AS SELECT * FROM nhs_demos.nhs_england_ml_demo.encounters;
CREATE OR REPLACE TABLE nhs_demos.nhs_england_ml_demo.condition_occurrence_ml AS SELECT * FROM nhs_demos.nhs_england_ml_demo.condition_occurrence;
CREATE OR REPLACE TABLE nhs_demos.nhs_england_ml_demo.conditions_ml AS SELECT * FROM nhs_demos.nhs_england_ml_demo.conditions;
