# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# Importing neaccery libary:

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# Schema for dataframe:
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# reading Json file
pit_stops_df = spark.read \
                    .schema(pit_stops_schema) \
                    .option("multiLine", True) \
                    .json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# Renaming and adding ingestion_date:
final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# writing final dataframe to processed layer:

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

