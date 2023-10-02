# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), True),
                                 StructField("driverRef", StringType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("code", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("dob", DateType(), True),
                                 StructField("nationality", StringType(), True),
                                 StructField("url", StringType(), True),
                                 ])

# COMMAND ----------

drivers_df = spark.read \
             .schema(drivers_schema)\
             .json("/mnt/covidreportingdevelopdl/databricks/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_with_column_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                            .withColumnRenamed("driverRef", "driver_ref")\
                                            .withColumn("ingestion_date", current_timestamp())\
                                            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_with_column_df)

# COMMAND ----------

drivers_final_df = drivers_with_column_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/covidreportingdevelopdl/databricks/processed/drivers")
