# Databricks notebook source
client_id="7c187439-bf28-48c9-a28b-9db10278118f"
tenenant_id="eecda088-6dda-4ddb-b23d-e3c6c91fddbb"
client_secret="3an8Q~SCxiK2ENtJeNSKUNp2mj6zHwONsTiYha8X"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://databricks@covidreportingdevelopdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingdevelopdl/databricks",
  extra_configs = configs)

# COMMAND ----------



# COMMAND ----------


display(dbutils.fs.ls("abfss://databricks@covidreportingdevelopdl.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/covidreportingdevelopdl/databricks"))

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://azureendtoend@covidreportingdevelopdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingdevelopdl/azureendtoend",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/covidreportingdevelopdl/azureendtoend"))

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://tokyo-olympic-data@covidreportingdevelopdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingdevelopdl/tokyo-olympic-data",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/covidreportingdevelopdl/tokyo-olympic-data"))
