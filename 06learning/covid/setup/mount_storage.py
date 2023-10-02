# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following data lake storage gen2 containers
# MAGIC 1. raw
# MAGIC 2. processed
# MAGIC 3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs
# MAGIC #### Please update the following 
# MAGIC - application-id
# MAGIC - service-credential
# MAGIC - directory-id

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "20430d94-16fa-4a50-8e9b-23ad46ec534f",
           "fs.azure.account.oauth2.client.secret": "nIL8Q~lhqz9S9s6wO14suyYWR7a-mY9I43UhLdrD",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/eecda088-6dda-4ddb-b23d-e3c6c91fddbb/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the raw container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@covidreportingdevelopdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingdevelopdl/raw",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@covidreportingdevelopdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingdevelopdl/processed",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the lookup container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://lookup@covidreportingdevelopdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingdevelopdl/lookup",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/covidreportingdevelopdl/lookup')
