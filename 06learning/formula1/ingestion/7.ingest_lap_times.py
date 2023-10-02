# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("poition", IntegerType(), True),                                 
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 ])

# COMMAND ----------

lap_times_df = spark.read \
             .schema(lap_times_schema)\
             .csv("/mnt/covidreportingdevelopdl/databricks/raw/lap_times/lap_times_split_*.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

final_lap_times_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_lap_times_df)

# COMMAND ----------

final_lap_times_df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/lap_times

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/pit_stops/part-00000-tid-2492048320919011476-97a3bfab-00ef-459b-9b0e-7092cfa8e1cf-12-1.c000.snappy.parquet"))

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/lap_times"))
