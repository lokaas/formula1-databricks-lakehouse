# Databricks notebook source
# Helper function to add the file metadata for ingestion (source file and ingestion timestamp)

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return(
    df.withColumn('ingestion_timestamp',F.current_timestamp())
    .withColumn('source_file' , F.col('_metadata.file_path'))
    )




# COMMAND ----------

def write_to_btonze(
    input_df,
    target_table,
    batch_id
) :
    final_df = input_df.withColumn("batch_id", F.lit(batch_id))
    (
    final_df
    .write
    .mode('overwrite')
    .format('delta')
    .partitionBy('batch_id')
    .option('replaceWhere',f"batch_id = '{batch_id}'")
    .saveAsTable(target_table)

)