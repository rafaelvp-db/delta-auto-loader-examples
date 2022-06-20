# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Bronze Layer

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#dbutils.fs.rm(f"dbfs:/home/{user}/demo/checkpoint/bronze", True) # for test purposes

# COMMAND ----------

# DBTITLE 1,Reading our data
#Starting our stream

df = spark.readStream.table("loan_raw_autoloader")

# COMMAND ----------

# DBTITLE 1,Filtering & Cleaning
#Some basic cleaning

df = df.filter("emp_title is not null") #Remove records with null emp_title
df = df.dropDuplicates(subset=["issue_d", "zip_code", "dti"]) #Drop duplicates based on a key composed by issue date, zip code and debt to income

# COMMAND ----------

# DBTITLE 1,Writing to Bronze
df.writeStream \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/bronze") \
  .toTable("loan_bronze_autoloader")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(1) FROM loan_bronze_autoloader

# COMMAND ----------


