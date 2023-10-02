-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo; 

-- COMMAND ----------

SHOW DATABASES;


-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from global_temp.gv_race_results
