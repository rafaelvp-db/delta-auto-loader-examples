# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Demo of Auto Loader
# MAGIC 
# MAGIC 1. Azure -- https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader
# MAGIC 2. AWS -- https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html
# MAGIC 
# MAGIC The benefits of Auto Loader are:
# MAGIC * Incrementally processes new files as they arrive in Amazon S3/Azure ADLS Gen2/Blob. You don’t need to manage any state information on what files arrived.
# MAGIC * Efficiently tracks the new files arriving by leveraging AWS SNS and AWS SQS [Azure AQS] without having to list all the files in a directory. This approach is scalable even with millions of files in a directory.
# MAGIC * Automatically sets up AWS SNS and AWS SQS [Azure AQS] required for incrementally processing the files, so you do not need to manually set up the queues.

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbutils.fs.rm(f"dbfs:/home/{user}/demo/", True)
dbutils.fs.rm(f"dbfs:/home/{user}/demo/checkpoint/", True)
dbutils.fs.rm(f"dbfs:/home/{user}/demo/auto-loader", True)
dbutils.fs.rm(f"dbfs:/home/{user}/demo/source-data/", True)

# COMMAND ----------

# DBTITLE 1,Load sample data and create test batches
source_df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load("dbfs:/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv")

# COMMAND ----------

display(source_df.limit(10))

# COMMAND ----------

import os
from pyspark.sql import functions as F

source_df = source_df.withColumn("issue_m", F.split(F.col("issue_d"), "-")[0])
source_df = source_df.withColumn("issue_y", F.split(F.col("issue_d"), "-")[1])
source_df.select("issue_m", "issue_y").display()

# COMMAND ----------

source_df.write.format("csv").partitionBy("issue_y", "issue_m").save(f"dbfs:/home/{user}/demo/source-data/")
print(os.listdir(f"/dbfs/home/{user}/demo/source-data/"))
print(os.listdir(f"/dbfs/home/{user}/demo/source-data/issue_y=18"))

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.region", "us-west-2") \
  .option("cloudFiles.validateOptions", "false") \
  .option("cloudFiles.includeExistingFiles", "true") \
  .schema(source_df.schema) \
  .load(f"dbfs:/home/{user}/demo/source-data")

# COMMAND ----------

df.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/") \
  .partitionBy("issue_y", "issue_m") \
  .start(f"dbfs:/home/{user}/demo/auto-loader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader: Loading Data from Specific Subfolder(s) with Glob Expressions
# MAGIC <hr/>
# MAGIC 
# MAGIC * It is possible to use Glob syntax when specifying the path for reading the file stream with Auto Loader, e.g.: `.load(f"dbfs:/home/$user%s/demo/source-data/batch-1*") //Reads only from folders starting with batch-1`
# MAGIC * There are other useful variations, for instance `[.load(f"dbfs:/home/$user%s/demo/source-data/issue_y=18/issue_m=[1|2]")` would give us only data pertaining to folders `1` and `2`.
# MAGIC * This can be quite useful in scenarios where you are migrating a batch job to a Spark Structured Streaming job with Auto Loader, and you have already ingested a big part of your raw files.
# MAGIC * Say for instance that we did a huge batch load for some of the files that we need to ingest, and now we want to start streaming the remaining ones. In our example, we can simulate for instance a case where all loans from **April** were already ingested, and now we want to start streaming from the **May** and **June** partitions.
# MAGIC * Let's put it into practice by reading only the files related to loans issued in June or May

# COMMAND ----------

df_jun_may = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.region", "us-west-2") \
    .option("cloudFiles.validateOptions", "false") \
    .option("cloudFiles.includeExistingFiles", "true") \
    .schema(source_df.schema) \
    .load(f"dbfs:/home/{user}/demo/source-data/issue_y=18/issue_m=[Jun|May]")

# COMMAND ----------

df_jun_may.writeStream \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/jun_may") \
  .toTable("loans_jun_may")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from loans_jun_may where issue_m = "Apr"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from loans_jun_may where issue_m in ("Jun", "May")
