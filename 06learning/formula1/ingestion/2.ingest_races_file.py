# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/raw/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), False),
                                     StructField("round", IntegerType(), False),
                                     StructField("circuitID", IntegerType(), False),
                                     StructField("name", StringType(), False),
                                     StructField("date", DateType(), False),
                                     StructField("time", StringType(), False),                                     
                                     StructField("url", StringType(), False)]
)

# COMMAND ----------

race_df = spark.read \
.option("header", True)\
.schema(race_schema)\
.csv("/mnt/covidreportingdevelopdl/databricks/raw/races.csv")

# COMMAND ----------

races_with_timestamp_df = race_df.withColumn("ingestion_date", current_timestamp())\
                          .withColumn("races_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year") , col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("races_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/covidreportingdevelopdl/databricks/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/races/part-00000-tid-5978004141981860545-e40fdad4-6274-4247-ac0d-5360db2bbf14-47-1.c000.snappy.parquet"))
