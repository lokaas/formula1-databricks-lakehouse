# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Race.csv file
# MAGIC
# MAGIC  1. Read the file using spark dataframe reader API
# MAGIC  2. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC     
# MAGIC  3. Write to bronze delta table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name =  f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read the csv file using the dataframe reader API

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.types import StructType, StructField, StringType , IntegerType , DateType

race_schema = StructType([
    StructField('season',       IntegerType()),
    StructField('round',        IntegerType()),
    StructField('url',          StringType()),
    StructField('raceName',     StringType()),
    StructField('date',         DateType()),
    StructField('circuitId',    StringType())
])


# COMMAND ----------

# DBTITLE 1,Read circuits csv with schema
race_df = (
    spark.read.
    format('csv')
    .option('header', 'true')
#    .option('inferSchema', 'true')
   .schema(race_schema)
    .load(source_file)
    )

# COMMAND ----------

race_df.show()

# COMMAND ----------

display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Add Metadata Columns
# MAGIC - Source File 
# MAGIC - Ingestion Timestamp

# COMMAND ----------


race__final_df = add_ingestion_metadata(race_df)



# COMMAND ----------

display(race__final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Erite to bronze delta table

# COMMAND ----------

# DBTITLE 1,Write to bronze delta table
(
    race__final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(table_name)

)

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %sql 
# MAGIC SELECT * FROM formula1.bronze.race;

# COMMAND ----------

display(spark.table('formula1.bronze.race'))

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

