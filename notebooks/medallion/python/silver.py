# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Silver Layer

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#dbutils.fs.rm(f"dbfs:/home/{user}/demo/checkpoint/silver", True) # for test purposes

# COMMAND ----------

# DBTITLE 1,Reading our data
#Starting our stream

df = spark.readStream.table("loan_bronze_autoloader")

# COMMAND ----------

# DBTITLE 1,Some extra cleaning
#Some extra cleaning

df = df.dropDuplicates(subset=["issue_d", "zip_code", "dti"]) #Drop duplicates based on a key composed by issue date, zip code and debt to income

# COMMAND ----------

# DBTITLE 1,Writing to Silver
df.writeStream \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/silver") \
  .toTable("loan_silver_autoloader")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(1) FROM loan_silver_autoloader
