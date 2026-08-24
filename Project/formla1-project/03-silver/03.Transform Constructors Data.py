# Databricks notebook source
# MAGIC %md
# MAGIC #  Transform Constructors Data 

# COMMAND ----------

# MAGIC %md
# MAGIC  1. Read bronze Constructors table
# MAGIC  2. Keep only the columns required for analytics (Drop url column)
# MAGIC  3. Standardise column names using snake_case ( constructorsID --> constructors_id )
# MAGIC  4. Rename columns to make them more meaningful ( name --> constructors_name )
# MAGIC  5. Remove duplicate records
# MAGIC  6. Transform values of column nationality to Title Case
# MAGIC  7. Write the transformed data to silver races table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read bronze races table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_df=spark.read.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

constructors_dropped_df=constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case ( raceName --> race_name , circuitId --> circuit_id )
# MAGIC - Rename columns to make them more meaningful ( date --> race_date )

# COMMAND ----------

# DBTITLE 1,Cell 11
constructors_renamed_df=(
    constructors_dropped_df
        .withColumnRenamed("constructorId", "constructor_id")
        .withColumnRenamed("Name", "constructor_name")
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Remove duplicate records

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(['constructor_id'])


# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 - Transform values of column nationality to Title Case

# COMMAND ----------

constructors_final_df=(
    constructors_distinct_df
        .withColumn('nationality',F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7 - Write the transformed data to silver races table

# COMMAND ----------

(
    constructors_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

