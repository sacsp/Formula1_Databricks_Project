# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest quaalifying.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# Importing neaccery libary:

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# Schema for dataframe 
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

# Read json file 
qualifying_df = spark.read \
                    .schema(qualifying_schema) \
                    .option("multiLine", True) \
                    .json(f"{raw_folder_path}/qualifying")


# COMMAND ----------

# reanming and adding ingestion_date
qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# writing final dataframe to processed layer:

qualifying_final_df.write.mode("overwrite")\
        .parquet(f"{processed_folder_path}/qualifying")

