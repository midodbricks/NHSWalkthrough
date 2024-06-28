# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC Load data for the demo

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data $catalog=nhs_demos $db=nhs_england_ml_demo