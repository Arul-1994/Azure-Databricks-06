# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitsId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)]
)

# COMMAND ----------

circuits_df = spark.read \
.option("header", True)\
.schema(circuits_schema)\
.csv("dbfs:/mnt/covidreportingdevelopdl/databricks/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitsId", "circuitRef","name", "location", "country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef , circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"),col("name"), col("location"), col("country"), col("lat"), col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitsId", "circuit_id")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/covidreportingdevelopdl/databricks/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/covidreportingdevelopdl/databricks/processed/circuits/part-00000-tid-2046706892344699736-0e82b195-8c1c-4351-86ae-8a51df8b046a-32-1.c000.snappy.parquet"))
