# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregations functions
# MAGIC

# COMMAND ----------

# Built-in Aggregation functions
race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

demo_df = race_result_df.filter("race_year == 2020")

# COMMAND ----------

# display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count('race_name')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

display(demo_df.filter(demo_df.driver_name == 'Lewis Hamilton').select(sum('points')))

# COMMAND ----------

demo_df.filter(demo_df.driver_name == 'Lewis Hamilton').select(count('race_name')).show()

# COMMAND ----------

demo_df.filter(demo_df.driver_name == 'Lewis Hamilton')\
    .select(sum('points').alias('Total_points')\
        , countDistinct('race_name').alias('Total_races')).show()

# COMMAND ----------

# OR
demo_df.filter(demo_df.driver_name == 'Lewis Hamilton')\
    .select(sum('points'), countDistinct('race_name')).withColumnRenamed("sum(points)", "Total_points").show()

# COMMAND ----------

display(demo_df.orderBy(asc('driver_name')))

# COMMAND ----------

demo_df\
    .groupBy('driver_name')\
    .agg(sum('points').alias('total_points'), \
        countDistinct('race_name').alias('distinct_races')\
        )\
    .show()

# COMMAND ----------

demo_df = race_result_df.filter('race_year in (2019, 2020)')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

grouped_demo_df = demo_df\
    .groupBy('race_year', 'driver_name')\
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('no_of_races'))

# COMMAND ----------

display(grouped_demo_df.orderBy(grouped_demo_df.race_year), asc(grouped_demo_df.total_points))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *


rankspec = Window.partitionBy('race_year').orderBy(desc('total_points'))

display(grouped_demo_df.withColumn('rank', rank().over(rankspec)))

# COMMAND ----------


