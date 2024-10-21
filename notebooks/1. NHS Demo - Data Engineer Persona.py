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
# MAGIC As an NHS analyst I've been tasked with creating a bespoke dataset for use in PMQs. The data involves patient details and conditions, and I need to join data across the complex data estate in order to create the results. I also need to present the data in a user friendly way so non-technical users and can perform analysis and ask natural questions of the data. This will involve the following type of activities:
# MAGIC
# MAGIC - Ingesting data stored in existing stores, such as DPS/CDP
# MAGIC - exploding out complex columns and data types
# MAGIC - joining to different datasets across diffenret NHS environments
# MAGIC - saving the dataset for downstream analysis
# MAGIC - Create a single unified dataset to present to my downstream consumers

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, schema_of_json

# Ingest data from the specified table
person_df = spark.table("nhs_demos.mido_pixel_fhir_demo.person")


# COMMAND ----------

display(person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Now to connect to external data (see Catalog Explorer)**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query data from UDAL
# MAGIC select * from nhs_demos.udal.udal_conditions limit 10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ingest data from the nhs_demos.mido_pixel_fhir_demo.person table
# MAGIC 2. Infer the json schema from the name column
# MAGIC 3. Add a new column representing the fully exploded name column and call it full_name
# MAGIC 4. Save it as a SQL view so I can use later on called vperson

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, schema_of_json

# Ingest data from the specified table
person_df = spark.table("nhs_demos.mido_pixel_fhir_demo.person")

# Collect a sample JSON string from the "name" column to infer the schema
sample_json = person_df.select("name").limit(1).collect()[0]["name"]
json_schema = schema_of_json(sample_json)

# Add a new column representing the fully exploded "name" column and rename it to "full_name"
person_df_with_full_name = person_df.withColumn("name_json", from_json(col("name"), json_schema)) \
                                    .withColumn("full_name", explode(col("name_json")))

# Save as a SQL view
person_df_with_full_name.createOrReplaceTempView("vperson")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from vperson

# COMMAND ----------

#here's one I made earlier...

from pyspark.sql.functions import col, explode, from_json, schema_of_json

person_df = spark.table("nhs_demos.mido_pixel_fhir_demo.person")

# Collect a sample JSON string from the "name" column
sample_json = person_df.select("name").limit(1).collect()[0]["name"]


# Infer the schema from the sample JSON string
json_schema = schema_of_json(sample_json)


person_df = person_df.withColumn("fullname", from_json(col("name"), json_schema)) \
  .select("*", explode(col("fullname").alias("full_name")))

person_df.createOrReplaceTempView("vperson")

display(spark.sql("select * from vperson"))



# COMMAND ----------

# MAGIC %md
# MAGIC Write me an expression to query the full_name column in vperson so I can extract person_id, gender_source_value and family field into a query, in sql.
# MAGIC
# MAGIC Save the query as a view called vpersonfull

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW vpersonfull AS
SELECT person_id, gender_source_value, full_name.family
FROM vperson
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vpersonfull

# COMMAND ----------

#HERE's ONE I MADE EARLIER

display(spark.sql("""
SELECT person_id, gender_source_value, fullname_exploded.family
FROM (
    SELECT *, explode(fullname) AS fullname_exploded
    FROM vperson
)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Join vpersonfull to nhs_demos.udal.udal_conditions
# MAGIC - return just the person_id, condition_status and the family fields
# MAGIC - save it to a table called nhs_demos.nhs_england_analytics_demo.person_conditions_today

# COMMAND ----------

# Join vpersonfull to nhs_demos.udal.udal_conditions and remove duplicate columns
joined_df = spark.table("vpersonfull").join(
    spark.table("nhs_demos.udal.udal_conditions"),
    "person_id",
    "inner"
).select("person_id", "condition_status", "family")

# Display the result
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # With our table saved, we can now view the final output:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the final table
# MAGIC SELECT * FROM nhs_demos.nhs_england_analytics_demo.person_conditions limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## other examples - explain, convert to something else

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

display(spark.sql("select * from vperson"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now we can view this in Catalog Explorer, see lineage, grant permissions and apply any metadata
