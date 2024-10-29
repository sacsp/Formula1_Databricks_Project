# Databricks notebook source
# Azure Storage Account Details
storage_acc_name = 'formula1gen2'
raw_folder_path = 'abfss://raw-formula1-container@formula1gen2.dfs.core.windows.net'
processed_folder_path = 'abfss://processed-formula1-container@formula1gen2.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation-formula1-container@formula1gen2.dfs.core.windows.net'

# SAS Token
# dbutils.secrets.help()
# dbutils.secrets.listScopes()
# dbutils.secrets.list(scope= 'formula1-scope')
# dbutils.secrets.get(scope='formula1-scope', key= 'formula1-dl-sas-token')
formula1dl_sas_token = dbutils.secrets.get(scope='formula1-scope', key= 'formula1-dl-sas-token')

# Setting up config for access storage account:
spark.conf.set(f"fs.azure.account.auth.type.{storage_acc_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_acc_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_acc_name}.dfs.core.windows.net",f"{formula1dl_sas_token}")






# COMMAND ----------


