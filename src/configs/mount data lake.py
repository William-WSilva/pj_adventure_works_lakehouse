# Databricks notebook source
# obter lista de segredos Azure
dbutils.secrets.list('ws_adb_secret_scope')

# COMMAND ----------

# ADLS
storage_acount_name = 'wsadlsadwprd'

# App Registration
client_id           = dbutils.secrets.get('ws_adb_secret_scope', 'client-id')
tenant_id           = dbutils.secrets.get('ws_adb_secret_scope', 'tenant-id')
client_secret       = dbutils.secrets.get('ws_adb_secret_scope', 'client-secret')

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

def mount_adls(container_name):
    mount_point = f"/mnt/{storage_acount_name}/{container_name}"

    # Verificar se o ponto já está montado
    montar_mount_point = True
    for i in dbutils.fs.mounts():
        if i.mountPoint == mount_point:
            montar_mount_point = False
            break

    # Se não estiver montado, faz o mount
    if  montar_mount_point:
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_acount_name}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs)
    else:
        print(f"O container já está montado: {mount_point}")

# COMMAND ----------

# Lista conteúdo se o mount funcionar
try:
    display(dbutils.fs.ls(f"/mnt/{storage_acount_name}/adw/"))
except Exception as e:
    print("Erro ao listar o conteúdo:", e)

