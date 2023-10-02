# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.covidreportingdevelopdl.dfs.core.windows.net",
    "HtBnXQ51TnCr3LVBWxtnXXXdPTes8Xr3EQiWN2b812/QCnF7PdOEAkniTN2b0CFJWFoaZXUcnODg+AStOwbFZw=="
)



# COMMAND ----------

display(dbutils.fs.ls("abfs://databricks@covidreportingdevelopdl.dfs.core.windows.net"))

# COMMAND ----------


