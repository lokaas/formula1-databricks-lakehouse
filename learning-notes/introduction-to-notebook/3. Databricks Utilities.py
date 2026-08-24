# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks Utilities
# MAGIC
# MAGIC - File system Utilities
# MAGIC - Secrest Utilities
# MAGIC - Widget Utilities
# MAGIC - Notebook Workflow Utilities

# COMMAND ----------

# MAGIC %md 
# MAGIC ### File system Utilities

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

