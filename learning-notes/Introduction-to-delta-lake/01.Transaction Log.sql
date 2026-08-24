-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Understanding Delta Lake Transaction Log
-- MAGIC Understand the cloud storage structure foe a delta table

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 1. Create Catalog and Schema for the Demo
-- MAGIC
-- MAGIC Catalog: demo
-- MAGIC
-- MAGIC Schema delta_lake

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo
MANAGED LOCATION 'abfss://demo@databrickscourseextdl145.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.delta_lake
MANAGED LOCATION 'abfss://demo@databrickscourseextdl145.dfs.core.windows.net/delta_lake'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create Delta Lake Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
(
    company_name    STRING,
    founded_date    DATE,
    country         STRING

)
USING DELTA


-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Insert Some Data

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES('APPLE', '1976-04-01', 'USA')

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Insert more Data

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES
    ('MICROSOFT',   '1975-04-04',    'USA'),
    ('GOOGLE',      '1998-09-04',     'USA'),
    ('AMAZON',      '1994-07-05',    'USA' )


-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

