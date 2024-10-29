# Databricks notebook source
# MAGIC %md
# MAGIC ##### Filter Transformations

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
races_df.show()

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <=5")
races_filtered_df.show()

# COMMAND ----------

races_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))
display(races_filtered_df.count()) 

# COMMAND ----------


