-- Databricks notebook source
-- MAGIC %md
-- MAGIC # History and Time Travel
-- MAGIC
-- MAGIC 1. Query Delta Lake table history.
-- MAGIC 2. Query pervious versions of the data.
-- MAGIC 3. Query data from a spevifiv time.
-- MAGIC 4. Restore data to a specific version.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC  1. Query Delta Lake table history.
-- MAGIC

-- COMMAND ----------

DESC HISTORY demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query pervious versions of the data.
-- MAGIC

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies
VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 3. Query data from a spevifiv time.
-- MAGIC

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies
TIMESTAMP AS OF '2026-08-10T16:41:24.000+00:00'

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies
TIMESTAMP AS OF '2026-08-10T16:45:50.000+00:00'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Restore data to a specific version.

-- COMMAND ----------

RESTORE TABLE demo.delta_lake.companies
 VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

DESC HISTORY demo.delta_lake.companies

-- COMMAND ----------

