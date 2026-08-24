# Databricks notebook source
# MAGIC %md
# MAGIC # Build Driver Standing 
# MAGIC
# MAGIC ## Sources
# MAGIC 1. fact_session_result
# MAGIC 2. dim_drivers
# MAGIC
# MAGIC ## Output Columns
# MAGIC 1. season
# MAGIC 2. driver id
# MAGIC 3. driver name
# MAGIC 4. nationality
# MAGIC 5. race starts
# MAGIC 6. total points
# MAGIC 7. number of wins
# MAGIC 8. number of podiums
# MAGIC 9. standing position

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formula1.gold.v_driver_standing
# MAGIC AS
# MAGIC WITH driver_session_summry
# MAGIC AS (
# MAGIC SELECT r.season,
# MAGIC         d.driver_id,
# MAGIC         d.driver_name,
# MAGIC         d.nationality,
# MAGIC         COUNT(*) AS race_starts,
# MAGIC         SUM(points) AS total_points,
# MAGIC         COUNT_IF(r.is_win) AS number_of_wins,
# MAGIC         COUNT_IF(r.is_podium) As number_of_podiums
# MAGIC     FROM formula1.gold.fact_session_results r
# MAGIC     join formula1.gold.dim_drivers d
# MAGIC         ON r.driver_id = d.driver_id
# MAGIC GROUP BY r.season,
# MAGIC         d.driver_id,
# MAGIC         d.driver_name,
# MAGIC         d.nationality)
# MAGIC SELECT season,
# MAGIC         driver_id,
# MAGIC         driver_name,
# MAGIC         nationality,
# MAGIC         RANK()OVER(PARTITION BY season ORDER BY total_points , number_of_wins DESC) AS standing,
# MAGIC         race_starts,
# MAGIC         total_points,
# MAGIC         number_of_wins,
# MAGIC         number_of_podiums
# MAGIC FROM driver_session_summry
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.gold.v_driver_standing WHERE season = 2025

# COMMAND ----------

