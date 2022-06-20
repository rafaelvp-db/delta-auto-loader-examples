# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Gold Layer

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#dbutils.fs.rm(f"dbfs:/home/{user}/demo/checkpoint/gold", True) # for test purposes

# COMMAND ----------

# DBTITLE 1,Reading our data
#Starting our stream

df = spark.readStream.table("loan_silver_autoloader")

# COMMAND ----------

# DBTITLE 1,Creating some basic aggregates
#Some extra cleaning

df_agg_addr_state_count = df.groupBy("addr_state").count()
df_agg_addr_state_avg_income = df.groupBy("addr_state").avg("annual_inc").withColumnRenamed("avg(annual_inc)", "avg_annual_inc")
df_agg_grade_count = df.groupBy("grade").count()
df_agg_emp_title_count = df.groupBy("emp_title").count()

# COMMAND ----------

# DBTITLE 1,Writing to Gold
#Quantity of loans per state
df_agg_addr_state_count.writeStream \
  .outputMode("complete") \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/loan_addr_state_count") \
  .option("delta.columnMapping.mode", "name") \
  .toTable("loan_addr_state_count")

#Quantity of loans per state
df_agg_addr_state_avg_income.writeStream \
  .outputMode("complete") \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/loan_addr_state_avg_income") \
  .option("delta.columnMapping.mode", "name") \
  .toTable("loan_addr_state_avg_income")

#Quantity of loans per state
df_agg_grade_count.writeStream \
  .outputMode("complete") \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/loan_grade_count") \
  .toTable("loan_grade_count")

#Quantity of loans per state
df_agg_emp_title_count.writeStream \
  .outputMode("complete") \
  .trigger(once=True) \
  .option("checkpointLocation", f"dbfs:/home/{user}/demo/checkpoint/loan_emp_title_count") \
  .toTable("loan_emp_title_count")
