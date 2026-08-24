# Databricks notebook source
# MAGIC %sql
# MAGIC WITH constructor_metrics AS
# MAGIC (
# MAGIC   SELECT constructor_name,
# MAGIC          SUM(race_starts) AS race_starts,
# MAGIC          SUM(number_of_wins) AS total_wins,
# MAGIC          SUM(number_of_podiums) AS total_podiums,
# MAGIC          SUM(CASE WHEN standing = 1 THEN 1 ELSE 0 END) AS total_championships
# MAGIC   FROM formula1.gold.constructor_standing
# MAGIC   GROUP BY constructor_name
# MAGIC   HAVING total_championships >= 1
# MAGIC )
# MAGIC
# MAGIC SELECT constructor_name,
# MAGIC        race_starts,
# MAGIC        total_wins,
# MAGIC        total_podiums,
# MAGIC        total_championships,
# MAGIC        (total_championships * 100) + (total_wins * 10) + (total_podiums * 3) AS greatness_score
# MAGIC FROM constructor_metrics
# MAGIC ORDER BY greatness_score DESC