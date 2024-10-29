# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# reading file from raw container
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time",StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])

races_df = spark.read.option("Header","True")\
                    .schema(races_schema)\
                    .csv(f"{raw_folder_path}/races.csv")


# display(races_df)


# COMMAND ----------

races_ingestion_date_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat_ws(' ', col('date'), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# display(races_ingestion_date_df)

# COMMAND ----------

# Selecting and renaming columns with formatted code
races_selected_df = races_ingestion_date_df.select(
    col("raceId").alias("race_id"), 
    col("year").alias("race_year"), 
    col("round"), 
    col("circuitId").alias("circuit_id"), 
    col("name"),
    col('ingestion_date'),
    col('race_timestamp')
)


# display(races_selected_df)

# COMMAND ----------

# writing final dataframe to processed layer:

races_selected_df.write.mode('overwrite') \
    .partitionBy('race_year') \
    .parquet(f"{processed_folder_path}/races")


# COMMAND ----------

# display(spark.read.parquet(f"abfss://{processed_container_name}@{storage_acc_name}.dfs.core.windows.net/races"))
