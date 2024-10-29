# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Join Transformations

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# will use these 2 races and circuits for Joins
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id < 70")\
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

 races_df = spark.read.parquet(f"{processed_folder_path}/races")\
    .filter("race_year = 2019")\
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

# races_df.count()
# circuits_df.count()

# circuits_df has 69 and races_df has 21 records

# COMMAND ----------

# MAGIC %md
# MAGIC #### inner join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)\
        .orderBy(circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Outer Join

# COMMAND ----------

# left Outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)\
        .orderBy(circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Right Outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)\
        .orderBy(circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Full Outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)\
        .orderBy(circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Semi Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)\
        .orderBy(circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Anti Join
race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Cross join
race_circuits_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits_df)
