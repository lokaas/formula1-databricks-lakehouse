# Databricks notebook source
# MAGIC %md
# MAGIC # Create Control Table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{control_schema}")

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{control_schema}.batch_control
    (
        batch_id STRING,
        status STRING,
        created_timestamp TIMESTAMP,
        updated_timestamp TIMESTAMP
    )
          """
          )

# COMMAND ----------

# %sql
# CREATE SCHEMA IF NOT EXISTS formula1_incr.control;

# CREATE TABLE IF NOT EXISTS formula1_incr.control.batch_control
# (
#     batch_id STRING,
#     status STRING,
#     created_timestamp TIMESTAMP,
#     updated_timestamp TIMESTAMP
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_incr.control.batch_control

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula1_incr.control.batch_control;