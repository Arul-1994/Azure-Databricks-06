# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("constructorId", IntegerType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("grid", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("positionText", StringType(), True),
                                 StructField("positionOrder", IntegerType(), True),
                                 StructField("points", FloatType(), True),
                                 StructField("laps", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 StructField("fastestLap", IntegerType(), True),
                                 StructField("rank", IntegerType(), True),
                                 StructField("fastestLapTime", StringType(), True),
                                 StructField("fastestLapSpeed", FloatType(), True),
                                 StructField("StatusID", StringType(), True),
                                 ])

# COMMAND ----------

results_df = spark.read \
             .schema(results_schema)\
             .json("/mnt/covidreportingdevelopdl/databricks/raw/results.json")

# COMMAND ----------

results_with_column_df = results_df.withColumnRenamed("resultId", "result_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("positionText", "position_test")\
.withColumnRenamed("positionOrder", "position_order")\
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(results_with_column_df)

# COMMAND ----------

results_final_df = results_with_column_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/results

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/results/part-00001-tid-724440678374348362-30f8190f-9f62-48ac-b6d2-07cde91a6d64-9-1.c000.snappy.parquet"))
