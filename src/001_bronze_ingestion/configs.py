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