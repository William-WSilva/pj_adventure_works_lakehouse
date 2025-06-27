# Databricks notebook source
dbutils.widgets.text('table_name','','')
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# MAGIC %run "./configs"

# COMMAND ----------

if not configs:
    print("error")

# COMMAND ----------

table_configs = configs[table_name]

print(table_configs)

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

table_configs

# COMMAND ----------

file_path = table_configs['file_path']
table_name = table_configs['table_name']
database_name = table_configs['database_name']

ingest_bronze_and_save(file_path=file_path, table_name=table_name, database_name=database_name)

# COMMAND ----------

dbutils.notebook.exit(f"tabela {table_name} processada com sucesso")

# COMMAND ----------

