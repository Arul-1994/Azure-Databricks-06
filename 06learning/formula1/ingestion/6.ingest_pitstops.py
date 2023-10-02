# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("stop", StringType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("duration", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 ])

# COMMAND ----------

pit_stops_schema_df = spark.read \
             .schema(pit_stops_schema)\
             .option("multiLine", True)\
             .json("/mnt/covidreportingdevelopdl/databricks/raw/pit_stops.json")

# COMMAND ----------

pit_stops_with_column__df = pit_stops_schema_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(pit_stops_with_column__df)

# COMMAND ----------

pit_stops_with_column__df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/pit_stops

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/pit_stops/part-00000-tid-2492048320919011476-97a3bfab-00ef-459b-9b0e-7092cfa8e1cf-12-1.c000.snappy.parquet"))
