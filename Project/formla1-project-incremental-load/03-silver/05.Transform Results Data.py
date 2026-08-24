# Databricks notebook source
# MAGIC %md
# MAGIC #  Transform Results Data 

# COMMAND ----------

# MAGIC %md
# MAGIC  1. Read bronze Results table
# MAGIC  2. Keep only the columns required for analytics (Drop url column)
# MAGIC  3. Standardise column names using snake_case ( constructorId --> constructor_id , 
# MAGIC  driverId --> driver_id , raceName --> race_name , positionText --> finish_position_text )
# MAGIC  4. Rename columns to make them more meaningful ( date --> race_date , grid --> grid_position , laps --> completed_laps , number --> car_number , position --> finish_position )
# MAGIC  5. Filter out rows where season , round , constructor_id or driver_id is null (business key validation)
# MAGIC  6. Remove duplicate records
# MAGIC  7. Transform values of column race_name to Title Case
# MAGIC  8. Write the transformed data to silver races table
# MAGIC
# MAGIC   Below changes are required to implement Incremental Load Prqcessing
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

bronze_table = f"{catalog_name}.{bronze_schema}.Results"
silver_table = f"{catalog_name}.{silver_schema}.Results"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read bronze races table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

Results_df=spark.read.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

Results_dropped_df=Results_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 & 4 - Standardise Column Names
# MAGIC -   Standardise column names using snake_case ( constructorId --> constructor_id , 
# MAGIC  driverId --> driver_id , raceName --> race_name , positionText --> finish_position_text )
# MAGIC  - Rename columns to make them more meaningful ( date --> race_date , grid --> grid_position , laps --> completed_laps , number --> car_number , position --> finish_position )

# COMMAND ----------

# DBTITLE 1,Cell 11
results_renamed_df = (
    Results_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
            "positionText": "final_position_text"
        })
)

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Filter out rows where season , round , constructor_id or driver_id is null (business key validation)

# COMMAND ----------

results_valid_df=(
    results_renamed_df
    .filter(
        F.col("season").isNotNull() &
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() &
        F.col("driver_id").isNotNull()
        
       
    )
)

# COMMAND ----------

display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 - Remove duplicate records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season" , "round" , "constructor_id" , "driver_id"])


# COMMAND ----------

display(results_valid_df.count() - results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7 - Transform values of column race_name to Title Case

# COMMAND ----------

results_final_df=(
    results_distinct_df
        .withColumn('race_name',F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 8 - Write the transformed data to silver results table

# COMMAND ----------

# (
#     results_final_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

write_to_silver(
    input_df=results_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_date",
        "race_name",
        "round",
        "season",
        "constructor_id",
        "driver_id",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

