# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Drivers.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# Importing neaccery libary:

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# Schema for dataframe: 

name_schema = StructType(fields = [
    StructField("forename" , StringType(), True),
    StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields= [
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# Reading Json file 

drivers_df = spark.read.schema(drivers_schema) \
                    .json(f"{raw_folder_path}/drivers.json")

# display(drivers_df)

# COMMAND ----------

# Rename columns, add ingestion_date and create new col name to dataframe  
drivers_final_df = drivers_df.withColumnRenamed("driverID", "driver_id") \
                                .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("name", concat_ws(" ", col("name.forename"), col("name.surname"))) \
                                .drop(drivers_df.url)


# display(drivers_final_df) 


# COMMAND ----------

drivers_final_df.write.mode("overwrite") \
                .parquet(f"{processed_folder_path}/drivers")


# display(spark.read.parquet(f"abfss://{processed_container_name}@{storage_acc_name}.dfs.core.windows.net/drivers"))
