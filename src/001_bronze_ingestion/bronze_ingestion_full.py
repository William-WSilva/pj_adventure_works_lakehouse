# Databricks notebook source
# MAGIC %run "./configs"

# COMMAND ----------

if not configs:
    print("error")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp


def ingest_bronze_and_save(file_path, table_name, database_name):
    # print(f'Lendo arquivo: {file_path}')
    # le o arquivo
    df = spark.read.format('parquet').load(file_path)
    # transforma
    df = df.withColumn('bronze_ingestion_timestamp', current_timestamp())
    # salvar
    df.write.mode('overwrite').option('overwriteSchema','True').format('delta').saveAsTable(f"{database_name}.{table_name}")
    # print(f"Salvo com sucesso a tabela: {database_name}.{table_name}")



# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed

# Função que processa um item de configuração
def process_config(item):
    file_path = configs[item]['file_path']
    database_name = configs[item]['database_name']
    table_name = configs[item]['table_name']
    ingest_bronze_and_save(file_path=file_path, table_name=table_name, database_name=database_name)

# Configura o número de threads (ajuste conforme necessário)
max_workers = 10

# Executor para gerenciar os threads
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(process_config, item): item for item in configs}

    # Coleta os resultados conforme cada tarefa é concluída
    for future in as_completed(futures):
        item = futures[future]
        try:
            future.result()
            print(f"{item} processado com sucesso.")
        except Exception as e:
            print(f"Erro ao processar {item}: {e}")