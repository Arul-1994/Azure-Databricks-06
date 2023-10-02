# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file_job", 0 , {"p_data_source": "Ergast_API"})

# COMMAND ----------

v_result
