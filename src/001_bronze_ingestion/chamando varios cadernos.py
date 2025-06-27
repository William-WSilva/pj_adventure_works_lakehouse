# Databricks notebook source
# Definindo os parâmetros
params = {
    "name": "Joao Vitor Nascimento",
    "idade": "wewwedsdsdsds"
}

# Chamando o caderno
result = dbutils.notebook.run("/Workspace/AdventureWorks/bronze_ingestion/teste parameters", 60, params)

# COMMAND ----------

