# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, sum, count, when, desc, rank
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

constructor_standing_df = race_results_df\
    .groupBy("race_year", "team")\
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))


# COMMAND ----------

display(constructor_standing_df.filter("race_year == 2020"))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))


# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

