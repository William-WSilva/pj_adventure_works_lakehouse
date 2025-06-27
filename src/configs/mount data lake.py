# Databricks notebook source
# ADLS
storage_account_name = "adlsadventureworksprd" 

# APP Registration
client_id = dbutils.secrets.get('adventure-works', 'client-id')
tenant_id = dbutils.secrets.get('adventure-works', 'tenant-id')
client_secret = dbutils.secrets.get('adventure-works', 'client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls('landing-zone')

# COMMAND ----------

mount_adls('prata')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/adlsadventureworksprd/prata')

# COMMAND ----------

mount_adls('controle')

# COMMAND ----------

mount_adls('bronze')

# COMMAND ----------

mount_adls('prata')

# COMMAND ----------

mount_adls('ouro')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsadventureworksprd/controle'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsadventureworksprd/bronze'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsadventureworksprd/prata'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsadventureworksprd/ouro'))

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsadventureworksprd/landing-zone/AdventureWorks/Full_load/Production'))

# COMMAND ----------

df = spark.read.parquet('/mnt/adlsadventureworksprd/landing-zone/AdventureWorks/Full_load/Production/Production_BillOfMaterials.parquet')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('delta').save('/mnt/adlsadventureworksprd/bronze/primeira_tabela')

# COMMAND ----------

