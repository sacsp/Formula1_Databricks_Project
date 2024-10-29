# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest lap_time.csv folder
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# Importing neaccery libary:

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# Schema for dataframe
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# Reading csv file 
lap_times_df = spark.read \
                    .schema(lap_times_schema) \
                    .csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

#Renaming and adding ingestion_date
final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# writing final dataframe to processed layer:
final_df.write.mode("overwrite")\
    .parquet(f"{processed_folder_path}/lap_time")

