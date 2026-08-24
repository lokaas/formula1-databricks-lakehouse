-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the project environment for Formula1 Project
-- MAGIC - 1. Create External Location databricks-course-ext-dl145-formula1-incr
-- MAGIC - 2. Create Catalog formla1_incr
-- MAGIC - 3. Create Schemas landing,bronze, Silver and Gold
-- MAGIC - 4. Create Volume Files in the landing schema 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Cloud Storge

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickscourseextdl145_formula1_incr
URL 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula1-incr container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog formla1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG  IF NOT EXISTS  formula1_incr
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/' 
    COMMENT 'This is the main Catalog for the Formula1 project' ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;

CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/silver';

CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/gold';

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG formula1_incr;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create volume Files

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@databrickscourseextdl145.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files

-- COMMAND ----------

