# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

qualifications_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("constructorId", IntegerType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("q1", StringType(), True),
                                 StructField("q2", StringType(), True),
                                 StructField("q3", StringType(), True),                                 
                                 ])

# COMMAND ----------

qualifications_df = spark.read \
             .schema(qualifications_schema)\
             .option("multiLine", True)\
             .json("/mnt/covidreportingdevelopdl/databricks/raw/qualifying")

# COMMAND ----------

qualifications_final__df = qualifications_df.withColumnRenamed("qualifyId", "qualify_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(qualifications_final__df)

# COMMAND ----------

qualifications_final__df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/qualifying

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/qualifying"))
