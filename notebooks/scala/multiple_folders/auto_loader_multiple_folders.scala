// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Demo of Auto Loader
// MAGIC 
// MAGIC 1. Azure -- https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader
// MAGIC 2. AWS -- https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html
// MAGIC 
// MAGIC The benefits of Auto Loader are:
// MAGIC * Incrementally processes new files as they arrive in Amazon S3/Azure ADLS Gen2/Blob. You don’t need to manage any state information on what files arrived.
// MAGIC * Efficiently tracks the new files arriving by leveraging AWS SNS and AWS SQS [Azure AQS] without having to list all the files in a directory. This approach is scalable even with millions of files in a directory.
// MAGIC * Automatically sets up AWS SNS and AWS SQS [Azure AQS] required for incrementally processing the files, so you do not need to manually set up the queues.

// COMMAND ----------

val user = dbutils.notebook.getContext().tags("user") 
dbutils.fs.rm(f"dbfs:/home/$user%s/demo/", true)
dbutils.fs.rm(f"dbfs:/home/$user%s/demo/checkpoint/", true)
dbutils.fs.rm(f"dbfs:/home/$user%s/demo/auto-loader", true)
dbutils.fs.rm(f"dbfs:/home/$user%s/demo/source-data/", true)

// COMMAND ----------

// DBTITLE 1,Load sample data and create test batches
val random = scala.util.Random

val sourceDF = (spark.read.format("csv")
                  .option("inferSchema", "true")
                  .option("header", "true")
                  .load("dbfs:/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv")
               )

val batch1 = sourceDF.filter($"grade"==="A")
val batch2 = sourceDF.filter($"grade"==="B")
val batch3 = sourceDF.filter($"grade"==="C")

// COMMAND ----------

display(sourceDF.limit(10))

// COMMAND ----------

val df = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.region", "us-west-2") 
            .option("cloudFiles.validateOptions", "false")
            .option("cloudFiles.includeExistingFiles", "true") 
            .schema(sourceDF.schema)
            .load(f"dbfs:/home/$user%s/demo/source-data")
         )

// COMMAND ----------

df.writeStream
  .format("delta")
  .option("checkpointLocation", f"dbfs:/home/$user%s/demo/checkpoint/")
  .start(f"dbfs:/home/$user%s/demo/auto-loader")

// COMMAND ----------

// DBTITLE 1,Write batch-1 Data to S3
for (i <- 1 to 5)
{
  batch1.write.parquet(s"""dbfs:/home/${user}/demo/source-data/batch-1${i}""")
}

display(dbutils.fs.ls(f"dbfs:/home/$user%s/demo/source-data/"))

// COMMAND ----------

display(sql(f"SELECT * FROM delta.`dbfs:/home/$user%s/demo/auto-loader`"))

// COMMAND ----------

display(sql(f"SELECT count(*) FROM delta.`dbfs:/home/$user%s/demo/auto-loader`"))

// COMMAND ----------

// DBTITLE 1,Write batch-2 data to S3
for (i <- 1 to 5)
{
  batch2.write.parquet(s"""dbfs:/home/${user}/demo/source-data/batch-2${i}""")
}

display(dbutils.fs.ls(f"dbfs:/home/$user%s/demo/source-data/"))

// COMMAND ----------

// DBTITLE 1,Check if Auto Ingest populated batch-2 data into Delta table
display(sql(f"SELECT * FROM delta.`dbfs:/home/$user%s/demo/auto-loader` WHERE grade = 'B'"))

// COMMAND ----------

display(sql(f"SELECT count(*) FROM delta.`dbfs:/home/$user%s/demo/auto-loader`"))

// COMMAND ----------

// DBTITLE 1,Write batch-3 data to S3
for (i <- 1 to 5)
{
  batch3.write.parquet(s"""dbfs:/home/${user}/demo/source-data/batch-3${i}""")
}

display(dbutils.fs.ls(f"dbfs:/home/$user%s/demo/source-data/"))

// COMMAND ----------

// DBTITLE 1,Check if Auto Ingest populated batch-3 data into Delta table
display(sql(f"SELECT * FROM delta.`dbfs:/home/$user%s/demo/auto-loader` WHERE grade = 'C'"))

// COMMAND ----------

display(sql(f"SELECT count(*) FROM delta.`dbfs:/home/$user%s/demo/auto-loader`"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Loading Data from Specific Subfolder(s)
// MAGIC 
// MAGIC * It is possible to use [Glob](https://en.wikipedia.org/wiki/Glob_(programming)) syntax when specifying the path for reading the file stream with Auto Loader, e.g.: `.load(f"dbfs:/home/$user%s/demo/source-data/batch-1*") //Reads only from folders starting with batch-1`
// MAGIC * This can be quite useful in scenarios where you are migrating a batch job to a Spark Structured Streaming job with Auto Loader, and you have already ingested a big part of your raw files.
// MAGIC * Let's put it into practice by reading only the files from **batch-2**

// COMMAND ----------

val df_batch_2 = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.region", "us-west-2") 
            .option("cloudFiles.validateOptions", "false")
            .option("cloudFiles.includeExistingFiles", "true") 
            .schema(sourceDF.schema)
            .load(f"dbfs:/home/$user%s/demo/source-data/batch-2*")
         )

// COMMAND ----------

df_batch_2.writeStream
  .option("checkpointLocation", f"dbfs:/home/$user%s/demo/checkpoint/batch2")
  .toTable("auto_loader_batch_2")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from auto_loader_batch_2 where grade = 'B'
