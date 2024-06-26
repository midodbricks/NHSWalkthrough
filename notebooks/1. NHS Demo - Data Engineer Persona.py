# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Data Engineer Persona  
# MAGIC
# MAGIC Showcases a typical NHS Data Engineer connecting to other data sources such as UDAL, Facts and Dimensions and creating tables for consumption by downstream streams
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Connect to UDAL
# MAGIC - Connect to Facts and Dimensions
# MAGIC - Connect to the SDE
# MAGIC - Create a single unified dataset to present to my downstream consumers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query data from UDAL
# MAGIC select * from nhs_udal_synapse.dbo.udal_condition limit 10
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, schema_of_json

person_df = spark.table("nhs_demos.mido_pixel_fhir_demo.person")

# Collect a sample JSON string from the "name" column
sample_json = person_df.select("name").limit(1).collect()[0]["name"]


# Infer the schema from the sample JSON string
json_schema = schema_of_json(sample_json)


person_df = person_df.withColumn("fullname", from_json(col("name"), json_schema)) \
  .select("*", explode(col("fullname").alias("full_name")))

person_df.createOrReplaceTempView("vperson")





# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --now query out in SQL
# MAGIC select person_id, gender_source_value,  exploded.family as Person_Name
# MAGIC from vperson
# MAGIC lateral view explode(vperson.fullname) as exploded

# COMMAND ----------

# MAGIC %sql
# MAGIC -- generate this from asking question - how do i write this to a table
# MAGIC CREATE TABLE IF NOT EXISTS nhs_demos.nhs_england_analytics_demo.person AS
# MAGIC SELECT person_id, gender_source_value, exploded.family AS Person_Name
# MAGIC FROM vperson
# MAGIC LATERAL VIEW explode(vperson.fullname) AS exploded

# COMMAND ----------

#question 1 - how can i join nhs_udal_synapse.dbo.udal_condition to vperson on person_id
#question 2 - show me to join nhs_udal_synapse.dbo.udal_condition to vperson on person_id and save the combined output to a person_conditions table in nhs_demos.nhs_england_analytics_demo database

# Alias the DataFrames for clarity
udal_condition_df = spark.table("nhs_udal_synapse.dbo.udal_condition").alias("udal")
vperson_df = spark.table("nhs_demos.nhs_england_analytics_demo.person").alias("vp")

# Join using the aliased DataFrames and their columns
joined_df = udal_condition_df.join(
    vperson_df, 
    udal_condition_df["udal.person_id"] == vperson_df["vp.person_id"], 
    "inner"
).drop(vperson_df["vp.person_id"]) \


#extend to drop other array cols - extend this to drop the fullname and col columns from joined_df before writing


# Saving the combined output to a table in the specified database
spark.sql("drop table if exists nhs_demos.nhs_england_analytics_demo.person_conditions")

joined_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("nhs_demos.nhs_england_analytics_demo.person_conditions")

# COMMAND ----------

# MAGIC %md
# MAGIC # With our table saved, we ca now view the final output:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the final table
# MAGIC SELECT * FROM nhs_demos.nhs_england_analytics_demo.person_conditions limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now we can view this in Catalog Explorer, see lineage, grant permissions and apply any metadata

# COMMAND ----------


