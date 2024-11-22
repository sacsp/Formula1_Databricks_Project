# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access dataframe using SQL
# MAGIC ##### Objectives
# MAGIC 1. Create Temp view on dataframe.
# MAGIC 2. Access the views from sql.
# MAGIC 3. Access the view from Python cell.
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

display(spark.sql("SELECT * FROM v_race_results"))

# COMMAND ----------

df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# To Create Global Temp view

df.createOrReplaceGlobalTempView("v_race_results_glb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.v_race_results_glb

# COMMAND ----------

display(spark.sql('select * from global_temp.v_race_results_glb'))

# COMMAND ----------


