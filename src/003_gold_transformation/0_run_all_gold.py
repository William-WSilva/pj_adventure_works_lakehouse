# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# Definição do dicionário com os notebooks da camada gold usando nomes simples dos notebooks
dict_tables = {
    "DimCurrency": {
        "active": 1,
        "notebook_path": "DimCurrency"
    },
    "DimCustomer": {
        "active": 1,
        "notebook_path": "DimCustomer"
    },
    "DimDate": {
        "active": 1,
        "notebook_path": "DimDate"
    },
    "DimGeography": {
        "active": 1,
        "notebook_path": "DimGeography"
    },
    "DimProduct": {
        "active": 1,
        "notebook_path": "DimProduct"
    },
    "DimPromotion": {
        "active": 1,
        "notebook_path": "DimPromotion"
    },
    "DimSalesReason": {
        "active": 1,
        "notebook_path": "DimSalesReason"
    },
    "DimSalesTerritory": {
        "active": 1,
        "notebook_path": "DimSalesTerritory"
    },
    "FactInternetSales": {
        "active": 1,
        "notebook_path": "FactInternetSales"
    },
    "FactInternetSalesReason": {
        "active": 1,
        "notebook_path": "FactInternetSalesReason"
    }
}

# COMMAND ----------

def run_notebook(config):
    """Função para executar o notebook e capturar o resultado."""
    try:
        result = dbutils.notebook.run(config['notebook_path'], 0)  # timeout = 0 indica espera indefinida
        print(f"Notebook {config['notebook_path']} executado com sucesso.")
        return result
    except Exception as e:
        print(f"Erro ao executar o notebook {config['notebook_path']}: {e}")
        return None

# COMMAND ----------

# Configura o número de threads (ajuste conforme necessário)
max_workers = 10

# Executor para gerenciar os threads
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submeter notebooks ativos para execução paralela
    futures = {executor.submit(run_notebook, config): table for table, config in dict_tables.items() if config['active'] == 1}

    # Coleta os resultados conforme cada tarefa é concluída
    for future in as_completed(futures):
        table = futures[future]
        try:
            result = future.result()
            print(f"Notebook para {table} processado com sucesso.")
        except Exception as e:
            print(f"Erro ao processar {table}: {e}")

# COMMAND ----------

# Código de backup para execução sequencial (caso necessário)
for table, config in dict_tables.items():
    if config['active'] == 1:
        print(f"Executando notebook para {table} em {config['notebook_path']}")
        result = dbutils.notebook.run(config['notebook_path'], 0)  # timeout = 0 indica espera indefinida
        print(f"Resultado para {table}: {result}")
