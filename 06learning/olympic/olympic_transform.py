# Databricks notebook source
spark

# COMMAND ----------

#Sample session not required for Databricks & cloud
#from pyspark.sql import SparkSession

#spark = SparkSession\
#        .builder\
#        .appName("Python Spark Sql basic example")\
#        .config("spark.some.config.option", "config_value")\
#        .getOrCreate()

# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", "True").load("dbfs:/mnt/covidreportingdevelopdl/tokyo-olympic-data/raw-data/athletes.csv")

# COMMAND ----------

display(df)
