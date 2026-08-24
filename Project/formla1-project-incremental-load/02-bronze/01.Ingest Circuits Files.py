# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file
# MAGIC
# MAGIC  1. Read the file using spark dataframe reader API
# MAGIC  2. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC     
# MAGIC  3. Write to bronze delta table
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

v_batch_id

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name =  f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

table_name

# COMMAND ----------

source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read the csv file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, StringType

circuits_schema = StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)
])


# COMMAND ----------

# DBTITLE 1,Read circuits csv with schema
circuits_df = (
    spark.read.
    format('csv')
    .option('header', 'true')
#    .option('inferSchema', 'true')
    .schema(circuits_schema)
    .load(source_file)
    )

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Add Metadata Columns
# MAGIC - Source File 
# MAGIC - Ingestion Timestamp

# COMMAND ----------



circuits__final_df = add_ingestion_metadata(circuits_df)



# COMMAND ----------

display(circuits__final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

 circuits_final_df = circuits__final_df.withColumn("batch_id", F.lit(v_batch_id))

# COMMAND ----------

# DBTITLE 1,Write to bronze delta table

#(
#    circuits_final_df
#   .write
#    .mode('overwrite')
#    .format('delta')
#    .partitionBy('batch_id')
#    .option('replaceWhere',f"batch = '{v_batch_id}'")
#    .saveAsTable(table_name)

#)

# COMMAND ----------

write_to_btonze(
    input_df=circuits_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %sql 
# MAGIC SELECT * FROM formula1_incr.bronze.circuits;

# COMMAND ----------

display(spark.table('formula1.bronze.circuits'))

# COMMAND ----------

display(spark.table(table_name))