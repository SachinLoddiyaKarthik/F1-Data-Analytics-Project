# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dlsg_account_key = dbutils.secrets.get(scope = 'formula1-scope', key ='formula1dlsg-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlsg.dfs.core.windows.net",
    formula1dlsg_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlsg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlsg.dfs.core.windows.net/circuits.csv"))