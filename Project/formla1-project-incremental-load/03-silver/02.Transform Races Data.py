# Databricks notebook source
# MAGIC %md
# MAGIC #  Transform Races Data 

# COMMAND ----------

# MAGIC %md
# MAGIC  1. Read bronze races table
# MAGIC  2. Keep only the columns required for analytics (Drop url column)
# MAGIC  3. Standardise column names using snake_case ( raceName --> race_name , circuitId --> circuit_id )
# MAGIC  4. Rename columns to make them more meaningful ( date --> race_date )
# MAGIC  5. Remove duplicate records
# MAGIC  6. Transform values of column race_name to Title Case
# MAGIC  7. Write the transformed data to silver races table
# MAGIC  
# MAGIC  Below changes are required to implement Incremental Load Prqcessing
# MAGIC ory
# MAGIC 1. Accept batch_id as a parameter to the notebook 
# MAGIC 2. Process data for only the batch_id being passed in (i.e., filter reading from bronze using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timestamp and batch_id to the silver table 
# MAGIC 4. Merge the processed data to the silver table created_timestamp should only be populated at the time of inserting/ creating the record. It should not be updated during the merge update. Ensure that we are not overwriting the data in silver table by older bronze data (re-run scenario)

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver_helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read bronze races table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_df=spark.read.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case ( raceName --> race_name , circuitId --> circuit_id )
# MAGIC - Rename columns to make them more meaningful ( date --> race_date )

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnRenamed("raceName", "race_name")
        .withColumnRenamed("circuitId", "circuit_id")
        .withColumnRenamed("date", "race_date")
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Remove duplicate records

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(['season','round'])


# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 - Transform values of column race_name to Title Case

# COMMAND ----------

races_final_df=(
    races_distinct_df
        .withColumn('race_name',F.initcap(F.col("race_Name")))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7 - Write the transformed data to silver races table

# COMMAND ----------

# (
#     races_final_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
merge_condition="t.season = s.season AND t.round = s.round" , 
columns_to_update=[
        "season",
        "round",
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)


# COMMAND ----------

display(spark.table(silver_table))