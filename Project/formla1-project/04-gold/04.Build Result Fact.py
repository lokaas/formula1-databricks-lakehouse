# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Dimension
# MAGIC
# MAGIC 1. Read silver results table 
# MAGIC 2. read silver sprints table 
# MAGIC 3. Add new column session_type with values Race or SPRINT
# MAGIC 4. UNION results and sprints
# MAGIC 5. Deriver additional columns 
# MAGIC     - is_win -> indicates that the driver own the race
# MAGIC     - is_podium -> indicates that the driver scored a podium result(1,2,3)
# MAGIC     - has_points -> indicaates that the driver has scored points
# MAGIC 5. Write the transformed data to gold fact_session_resilts table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read Source table
# MAGIC - silver.results
# MAGIC - silver.sprints

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .withColumn("session_type",F.lit("RACE"))
        .drop("race_name" , "race_date" , "ingestion_timestamp" , "source_file")

)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .withColumn("session_type",F.lit("SPRINT"))
        .drop("race_name" , "race_date" , "ingestion_timestamp" , "source_file")

)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - UNION results and sprints
# MAGIC

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

display(results_sprints_df)


# COMMAND ----------

# MAGIC %md
# MAGIC  Step 3 - Add derived columns
# MAGIC
# MAGIC 1. is_win -> indicates that the driver own the race
# MAGIC 2. is_podium -> indicates that the driver scored a podium result(1,2,3)
# MAGIC 3. has_points -> indicaates that the driver has scored points

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
    .withColumn("is_win", F.col("final_position") == 1)
    .withColumn("is_podium" , F.col("final_position").between(1,3))
    .withColumn("has_points", F.col("points") > 0)
   
 
)

# COMMAND ----------

display(fact_session_results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Write the transformed data to gold fact_session_resilts table

# COMMAND ----------

(
    fact_session_results_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))