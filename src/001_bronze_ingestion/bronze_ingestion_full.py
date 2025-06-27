# Databricks notebook source

# MAGIC %run "/Workspace/Users/roseaneinacio@nw5y.onmicrosoft.com/ws_pj_adventure_works_lakehouse/src/001_bronze_ingestion/configs"

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