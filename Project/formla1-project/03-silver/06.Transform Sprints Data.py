# Databricks notebook source
# MAGIC %md
# MAGIC #  Transform Sprints Data 

# COMMAND ----------

# MAGIC %md
# MAGIC  1. Read bronze Results table
# MAGIC  2. Keep only the columns required for analytics (Drop url column)
# MAGIC  3. Standardise column names using snake_case ( constructorId --> constructor_id , driverId --> driver_id , raceName --> race_name , positionText --> finish_position_text )
# MAGIC  4. Rename columns to make them more meaningful ( date --> race_date , grid --> grid_position , laps --> completed_laps , number --> car_number , position --> finish_position )
# MAGIC  5. Filter out rows where season , round , constructor_id or driver_id is null (business key validation)
# MAGIC  6. Remove duplicate records
# MAGIC  7. Transform values of column race_name to Title Case
# MAGIC  8. Write the transformed data to silver races table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.Sprints"
silver_table = f"{catalog_name}.{silver_schema}.Sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read bronze races table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

Sprints_df=spark.read.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

Sprints_dropped_df=Sprints_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 & 4 - Standardise Column Names
# MAGIC -  Standardise column names using snake_case ( constructorId --> constructor_id , driverId --> driver_id , raceName --> race_name , positionText --> finish_position_text )
# MAGIC  - Rename columns to make them more meaningful ( date --> race_date , grid --> grid_position , laps --> completed_laps , number --> car_number , position --> finish_position )

# COMMAND ----------

# DBTITLE 1,Cell 11
Sprints_renamed_df = (
    Sprints_dropped_df
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

display(Sprints_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Filter out rows where season , round , constructor_id or driver_id is null (business key validation)

# COMMAND ----------

Sprints_valid_df=(
    Sprints_renamed_df
    .filter(
        F.col("season").isNotNull() &
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() &
        F.col("driver_id").isNotNull()
        
       
    )
)

# COMMAND ----------

display(Sprints_renamed_df.count() - Sprints_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 - Remove duplicate records

# COMMAND ----------

Sprints_distinct_df = Sprints_valid_df.dropDuplicates(["season" , "round" , "constructor_id" , "driver_id"])


# COMMAND ----------

display(Sprints_valid_df.count() - Sprints_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7 - Transform values of column race_name to Title Case

# COMMAND ----------

Sprints_final_df=(
    Sprints_distinct_df
        .withColumn('race_name',F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(Sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 8 - Write the transformed data to silver results table

# COMMAND ----------

(
    Sprints_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

