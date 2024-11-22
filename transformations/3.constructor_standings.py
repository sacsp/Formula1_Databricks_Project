# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# display(race_results_df)

# COMMAND ----------

team_standings_df = race_results_df\
    .groupBy('race_year', 'team')\
    .agg(sum('points').alias('total_points'),\
        count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

# display(team_standings_df.filter('race_year == 2020'))

# COMMAND ----------

team_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df = team_standings_df.withColumn('rank', rank().over(team_rank_spec))

# COMMAND ----------

# display(final_df.filter('race_year == 2020' ))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


