# Databricks notebook source
# MAGIC %md
# MAGIC ### %fs : Rusn file system commands

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

# MAGIC %md 
# MAGIC ### %sh :Run shell commands (Driver Node only)

# COMMAND ----------

# MAGIC %sh ps

# COMMAND ----------

# MAGIC %md
# MAGIC ### %pip : Install Python Libraris

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ### %Run : Include / Import another notebook into the current notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Users/lokaaref3@gmail.com/databricks-course/introduxtion-to-notebook/2.1 Enciroment Variables and Functions"

# COMMAND ----------

# MAGIC %run "./2.1 Enciroment Variables and Functions"

# COMMAND ----------

