# Databricks notebook source
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC
# MAGIC 1. Read silver races table
# MAGIC 2. Read silver circuits table
# MAGIC 3. Join the data from  races with circuits using circuits_id
# MAGIC 4. Select the required columns 
# MAGIC     - races.season
# MAGIC     - races.round
# MAGIC     - races.race_name
# MAGIC     - races.race_date
# MAGIC     - circuits.circuit_name
# MAGIC     - circuits.locality
# MAGIC     - circuits.country
# MAGIC 5. Write the transformed data to golf dim_races table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read Source tables
# MAGIC - circuits 
# MAGIC - races

# COMMAND ----------

circuits_df=spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df=spark.table(f"{catalog_name}.{silver_schema}.races")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 - join races with circuits using circuit_id
# MAGIC
# MAGIC Select the following columns
# MAGIC 1. races.season
# MAGIC 2. races.round
# MAGIC 3. races.round_name
# MAGIC 4. races.race_date
# MAGIC 5. circuits.cirsuit_name
# MAGIC 6. circuits.locality
# MAGIC 7. circuits.country

# COMMAND ----------

dim_races_df=(
    races_df
    .join(
        circuits_df,
        races_df.circuit_id == circuits_df.circuit_id,
        "inner"
        )
        .select(
          races_df.season,
          races_df.round,
          races_df.race_name,
          races_df.race_date,
          circuits_df.circuit_name,
          circuits_df.locality,
          circuits_df.country,
        )
          
          )


# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write the transformed data to the gold dim_races table

# COMMAND ----------

(
    dim_races_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

