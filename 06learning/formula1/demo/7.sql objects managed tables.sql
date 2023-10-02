-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
-- MAGIC from pyspark.sql.functions import col, sum, count, when, desc, rank
-- MAGIC from pyspark.sql.functions import current_timestamp, lit
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")
-- MAGIC

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT * from demo.race_results_python;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

SELECT * from demo.race_results_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/covidreportingdevelopdl/databricks/presentation/race_results_ext_sql" # Not needed for managed table
#Register table in Hive metastore

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * from demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_result
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2019;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_result
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2019;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * from global_temp.gv_race_result

-- COMMAND ----------

CREATE OR REPLACE  VIEW pv_race_result
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2019;

-- COMMAND ----------

DESCRIBE EXTENDED pv_race_result;
