-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Configure Acess to cloud Storge Via Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Cloud Storge

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://demo@databrickscourseextdl145.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickscourseextdl145_demo
URL 'abfss://demo@databrickscourseextdl145.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the demo container';