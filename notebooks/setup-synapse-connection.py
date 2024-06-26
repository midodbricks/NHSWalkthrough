# Databricks notebook source
# MAGIC %md-sandbox # Databricks - Unified Analytics Platform
# MAGIC
# MAGIC **Fast, Easy, Collaborative Analytics Platform based on Apache Spark**
# MAGIC </p>
# MAGIC
# MAGIC <div><img src="https://oneenvstorage.blob.core.windows.net/images/azure_architecture.png" style="height:450px"/></div>
# MAGIC
# MAGIC </p>
# MAGIC   
# MAGIC * **Delta Lake brings Reliability, Quality and Performance to your Data Platform**
# MAGIC   * ACID transactions - Multiple writers can simultaneously modify a data set and see consistent views.
# MAGIC   * Integrate BATCH and STREAMING data
# MAGIC   * Full DML Support
# MAGIC   * TIME TRAVEL - Look back on how data looked like in the past

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Extract batch and streaming data
# MAGIC * User data is batch, and Fitness Tracker data is streamed

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/iot-stream/data-user/

# COMMAND ----------

# MAGIC %fs rm -r /mnt/deltalake/fitness

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.types import DateType

# read CSV user data
userData = spark.read\
                .options(header='true', inferSchema='true')\
                .csv('/databricks-datasets/iot-stream/data-user/')
userData.createOrReplaceTempView("usersTemp")
display(userData)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Load into Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists fitness location  '/mnt/deltalake/fitness'

# COMMAND ----------

# MAGIC %sql
# MAGIC use fitness;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tracker_users;
# MAGIC
# MAGIC CREATE TABLE tracker_users
# MAGIC --USING delta
# MAGIC LOCATION '/mnt/deltalake/fitness/tracker_users'
# MAGIC  AS
# MAGIC  SELECT * from usersTemp;

# COMMAND ----------

# MAGIC %sql describe tracker_users

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tracker_users limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail tracker_users

# COMMAND ----------

# MAGIC %fs ls /mnt/deltalake/fitness/tracker_users/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write to Azure Synapse

# COMMAND ----------

# MAGIC %md
# MAGIC Please ensure oneenv-synapse under oneenv resource group in Azure Field Engg subscription is running. By default it will be paused, you need to click "Resume" before running this test

# COMMAND ----------

#STORAGE_KEY = dbutils.secrets.get(scope = "om-oneenv", key = "storage-key1")
STORAGE_KEY = "Ps+4dBrANErifAyIHviPEludGHZJbw1sm3P9Lx5vhKMEBK7k/36LV35qcC7/8ChkUzLWQKtIMU/z6CxCRciflg=="
#SQLPWD =  dbutils.secrets.get(scope = "oneenvkeys", key = "sqladmin-password")

# COMMAND ----------

print(SQLPWD[0:-1])
print(SQLPWD[1:])

# COMMAND ----------

spark.sql("""SET fs.azure.account.key.oneenvstorage.blob.core.windows.net={} """.format(STORAGE_KEY))

# COMMAND ----------

from datetime import datetime

df = spark.createDataFrame([
  [1, 'foo', datetime.strptime('0001-01-02', '%Y-%m-%d')],
], schema='id integer, foo string, date_ timestamp')

df.display()

# COMMAND ----------

df = spark.table('nhs_demos.mido_pixel_fhir_demo.condition')

# COMMAND ----------

df.write \
       .format("com.databricks.spark.sqldw") \
       .mode("overwrite") \
       .option("url", "jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=oneenv-synapse;user=oneenvadmin@oneenvsql;password=0n3env4d#in;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30") \
       .option("tempDir", "wasbs://synapse@oneenvstorage.blob.core.windows.net/tmp") \
       .option("forwardSparkAzureStorageCredentials", "true") \
       .option("dbTable", "dbo.udal_condition") \
       .save()

# COMMAND ----------

synapseUser =(spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=oneenv-synapse;user=oneenvadmin@oneenvsql;password="+SQLPWD+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30")
  .option("tempDir", "wasbs://synapse@oneenvstorage.blob.core.windows.net/tmp")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.date_test")
  .load())

display(synapseUser)

# COMMAND ----------

import pyodbc

