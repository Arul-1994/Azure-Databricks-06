# Databricks notebook source
input_path = "/mnt/covidreportingdevelopdl/azureendtoend/bronze/parquet/SalesLT/Address/Address.parquet"

# COMMAND ----------

#dbutils.fs.ls("/mnt/covidreportingdevelopdl/azureendtoend/bronze/SalesLT/")
dbutils.fs.ls(input_path)

# COMMAND ----------

df = spark.read.format("parquet").load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), 'UTC'), "yyyy-MM-dd"))




# COMMAND ----------

table_name = []

for i in dbutils.fs.ls("/mnt/covidreportingdevelopdl/azureendtoend/bronze/parquet/SalesLT/"):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

for i in table_name:
    path = '/mnt/covidreportingdevelopdl/azureendtoend/bronze/parquet/SalesLT/' + i + '/' + i + '.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if 'Date' in col or 'date' in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), 'UTC'), "yyyy-MM-dd"))

    output_path = '/mnt/covidreportingdevelopdl/azureendtoend/silver/SalesLT/' +i +'/'
    df.write.format('delta').mode("overwrite").save(output_path)


# COMMAND ----------

display(df)
