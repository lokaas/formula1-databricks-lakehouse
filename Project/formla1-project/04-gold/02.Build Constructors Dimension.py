# Databricks notebook source
# MAGIC %md
# MAGIC Build Constructors Dimension
# MAGIC
# MAGIC 1. Read silver Constructors table 
# MAGIC 2. read gold ref_nationality_region table
# MAGIC 3. join the data from Constructors with ref_nationality_region using nationality
# MAGIC 4. Select the required columns 
# MAGIC     - Constructors.Constructors_id
# MAGIC     - Constructors.Constructors_name
# MAGIC     - Constructors.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC 5. Write the transformed data to gold dim_Constructors table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_Constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read Source table
# MAGIC - silver.constructors
# MAGIC - gold.ref_nationality_region

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df=spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 2 - join the data from Constructors with ref_nationality_region using nationality
# MAGIC
# MAGIC 1. Constructors.Constructors_id
# MAGIC 2. Constructors.Constructors_name
# MAGIC 3. Constructors.nationality
# MAGIC 4. ref_nationality_region.region

# COMMAND ----------

from pyspark.sql.functions import col
dim_constructors_df =(

    constructors_df
    .join
    (
        ref_nationality_region_df,
        constructors_df.nationality == ref_nationality_region_df.nationality,
        "left"

    )
    .select(
        constructors_df.constructor_id,
        col("constructor_name"),
        constructors_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
   
        
        )

    
)

# COMMAND ----------

display(dim_constructors_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Write the transformed data to gold dim_Constructors table

# COMMAND ----------

(
    dim_constructors_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

