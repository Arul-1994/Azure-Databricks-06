# Databricks notebook source
client_id="7c187439-bf28-48c9-a28b-9db10278118f"
tenenant_id="eecda088-6dda-4ddb-b23d-e3c6c91fddbb"
client_secret="3an8Q~SCxiK2ENtJeNSKUNp2mj6zHwONsTiYha8X"

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.covidreportingdevelopdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.covidreportingdevelopdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.covidreportingdevelopdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.covidreportingdevelopdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.covidreportingdevelopdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenenant_id}/oauth2/token")



# COMMAND ----------

display(dbutils.fs.ls("abfs://databricks@covidreportingdevelopdl.dfs.core.windows.net"))

# COMMAND ----------


