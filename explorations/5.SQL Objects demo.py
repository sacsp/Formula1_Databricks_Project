# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE if NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database(); 

# COMMAND ----------

# MAGIC %sql
# MAGIC show Tables in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating Manged Tables using python and sql

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

df.write.format("Delta").saveAsTable("demo.race_result_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED race_result_py;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.race_result_py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table race_results_sql 
# MAGIC AS 
# MAGIC select * from race_result_py
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Creating External Tables using python and sql

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------


