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
# MAGIC
# MAGIC Below Changes are required to implement incremental load processing
# MAGIC
# MAGIC 1. Accept batch_id as parameter to the notebook
# MAGIC 2. Process data for only the batch_id being passed in (i.e., filter reading from silver using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timeatamp to the gold table
# MAGIC 4. Merge the processed data to the gold table 
# MAGIC     - Created_timestamp should only be populated at the tome of inserting/ creating the record. it should not be updated during the merge 
# MAGIC       update 

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/04.gold_helpers

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

drivers_df = (
spark.table(f"{catalog_name}.{silver_schema}.drivers")
    .filter(F.col("batch_id") == v_batch_id)

)


# COMMAND ----------

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

# (
#     dim_drivers_df
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(target_table)
# )

# COMMAND ----------

write_to_gold(
    input_df=dim_drivers_df,
    target_table=target_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_id",
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))