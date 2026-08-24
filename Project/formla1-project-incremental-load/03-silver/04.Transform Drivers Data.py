# Databricks notebook source
# MAGIC %md
# MAGIC #  Transform Drivers Data 

# COMMAND ----------

# MAGIC %md
# MAGIC  1. Read bronze Drivers table
# MAGIC  2. Keep only the columns required for analytics (Drop url column)
# MAGIC  3. Standardise column names using snake_case ( driverId --> driver_id , dateOfBirth --> date_of_birth )
# MAGIC  4. Concatenate name.givenName and name.familyName to create a new column called driver_name and transform the value to Title Case
# MAGIC  5. Remove duplicate records
# MAGIC  6. Transform values of column nationality to Title Case
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

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read bronze races table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_df=spark.read.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

drivers_dropped_df=drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3  - Standardise Column Names
# MAGIC - Standardise column names using snake_case ( driverId --> driver_id , dateOfBirth --> date_of_birth )

# COMMAND ----------

# DBTITLE 1,Cell 11
drivers_renamed_df=(
    drivers_dropped_df
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("dateOfBirth", "date_of_birth")
)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Concatenate name.givenName and name.familyName to create a new column called driver_name 

# COMMAND ----------

drivers_concatenated_df=(
    drivers_renamed_df
        .withColumn("driver_name", 
                    F.initcap(F.concat_ws(" ",F.col("name.givenName"), F.col("name.familyName"))))
        .drop("name")
)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Remove duplicate records

# COMMAND ----------

drivers_distinct_df = (
    drivers_concatenated_df
        .dropDuplicates(["driver_id"])
)

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 - Transform values of column nationality to Title Case

# COMMAND ----------

drivers_final_df=(
    drivers_distinct_df
        .withColumn('nationality',F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7 - Write the transformed data to silver races table

# COMMAND ----------

# (
#     drivers_final_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_id",
        "date_of_birth",
        "nationality",
        "driver_name",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))