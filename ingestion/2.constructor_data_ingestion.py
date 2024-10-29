# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructor.json file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# Importing neaccery libary:

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Reading Json file 

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


constructor_df = spark.read \
                    .schema(constructor_schema) \
                    .json(f"{raw_folder_path}/constructors.json")


# display(constructor_df)

# COMMAND ----------

# Drop unwanted columns from the dataframe

constructor_droped_df = constructor_df.drop(constructor_df.url)

# Renaming and adding ingestion_date to dataframe

constructor_final_df = constructor_droped_df.withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("constructorRef", "constructor_ref") \
                                    .withColumn("ingestion_date", current_timestamp())

# display(constructor_final_df)

# COMMAND ----------

# writing final dataframe to processed layer:

constructor_final_df.write.mode("overwrite").parquet(
    f"{processed_folder_path}/constructor"
)

# COMMAND ----------

# display(spark.read.parquet(f"abfss://{processed_container_name}@{storage_acc_name}.dfs.core.windows.net/constructor")
