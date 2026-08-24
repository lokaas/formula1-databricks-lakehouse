# Databricks notebook source
# MAGIC %md
# MAGIC # Build Drivers Dimension
# MAGIC
# MAGIC 1. Read silver Drivers table 
# MAGIC 2. read gold ref_nationality_region table
# MAGIC 3. join the data from Constructors with ref_nationality_region using nationality
# MAGIC 4. Select the required columns 
# MAGIC     - drivers.Drivers
# MAGIC     - drivers.Drivers_name
# MAGIC     - drivers.date_of_brith
# MAGIC     - drivers.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC 5. Write the transformed data to gold dim_drivers table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read Source table
# MAGIC - silver.drivers
# MAGIC - gold.ref_nationality_region

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df=spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - join the data from Constructors with ref_nationality_region using nationality
# MAGIC
# MAGIC 1. drivers.Drivers
# MAGIC 2. drivers.Drivers_name
# MAGIC 3. drivers.date_of_brith
# MAGIC 4. drivers.nationality
# MAGIC 5. ref_nationality_region.region

# COMMAND ----------

dim_drivers_df =(
    drivers_df
    .join
    (
        ref_nationality_region_df,
        drivers_df.nationality == ref_nationality_region_df.nationality,
        "left"

    )
    .select(
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
   
        
        )

    
)

# COMMAND ----------

display(dim_drivers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Write the transformed data to gold dim_drivers table

# COMMAND ----------

(
    dim_drivers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

