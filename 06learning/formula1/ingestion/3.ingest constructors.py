# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING "

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema)\
    .json("/mnt/covidreportingdevelopdl/databricks/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")
constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                            .withColumnRenamed("constructorRef", "constructor_ref")\
                                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/constructors")
