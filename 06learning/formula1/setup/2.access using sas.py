# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.covidreportingdevelopdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.covidreportingdevelopdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.covidreportingdevelopdl.dfs.core.windows.net", "sp=rl&st=2023-09-09T06:43:07Z&se=2023-09-09T14:43:07Z&spr=https&sv=2022-11-02&sr=c&sig=yIOnrLUjE3S70p0Pxn0Id7fG3iY1dnzEvdDPiPpo7kI%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfs://databricks@covidreportingdevelopdl.dfs.core.windows.net"))
