# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuit.csv file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# Importing neaccery libary:
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Reading file from raw container
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)
                                    ])

circuits_df = spark.read.option("Header","True")\
                        .schema(circuits_schema)\
                        .csv(f"{raw_folder_path}/circuits.csv")


# circuits_df.show()

# COMMAND ----------

# Selecting and Renaming columns with formatted code
circuits_renamed_df = circuits_df.select(
    col("circuitId").alias("circuit_id"), 
    col("circuitRef").alias("circuit_ref"), 
    col("name"), 
    col("location"), 
    col("country"),
    col("lat").alias("latitude"), 
    col("lng").alias("longitude"), 
    col("alt").alias("altitude")
)


# display(circuits_renamed_df)

# COMMAND ----------

#  Add ingestion date to dataframe
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# circuits_final_df.show()

# COMMAND ----------

circuits_final_df.write.mode("overwrite")\
                .parquet(f"{processed_folder_path}/circuits")


# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/circuits"))
