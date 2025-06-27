# Databricks notebook source

# criar um dicionario com todos os schemas(pastas) e tabelas para processamento 
def generate_ingestion_dict(path, database_name, input_format):
    ingestion_tables = {}
    folders = dbutils.fs.ls(path)

    for folder in folders:
        # Verifica se é uma pasta
        if folder.isDir():
            subfolder_path = folder.path
            files = dbutils.fs.ls(subfolder_path)

            for file in files:
                # Verifica se é um arquivo parquet
                if file.name.endswith('.parquet'):
                    table_name = file.name.replace('.parquet', '')
                    file_path = f"{subfolder_path.replace('dbfs:', '')}/{file.name}"

                    ingestion_tables[table_name] = {
                        'database_name': database_name,
                        'file_path': file_path,
                        'table_name': table_name,
                        'input_format': input_format
                    }

    return ingestion_tables


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

def process_table(table_info: dict) -> str:
    """
    Processa uma tabela com base nas informações do dicionário.
    """
    try:
        ingest_bronze_and_save(
            file_path=table_info['file_path'],
            table_name=table_info['table_name'],
            database_name=table_info['database_name']
        )
        return f"✅ Ingestão OK: {table_info['table_name']}"
    except Exception as e:
        return f"❌ Ingestão falhou: {table_info['table_name']}: {e}"

# Número de threads
max_threads = 10

# Lista para armazenar futures
futures = []
results = []

print(f"🚀 Iniciando ingestão com {max_threads} threads...\n")

# Executor em paralelo com append
with ThreadPoolExecutor(max_workers=max_threads) as executor:
    for table_info in generate_ingestion_dict.values():
        print(f"🕓 Agendando: {table_info['table_name']}")
        futures.append(executor.submit(process_table, table_info))

    for future in as_completed(futures):
        result = future.result()
        print(result)
        results.append(result)