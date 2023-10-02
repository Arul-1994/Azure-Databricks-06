# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")
#race_results_df.createOrReplaceTempView("v_race_results")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from v_race_results where race_year = 2020

# COMMAND ----------

p_race_year=2020

# COMMAND ----------

race_results_df1 = spark.sql(f"SELECT * from v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_df1)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results
