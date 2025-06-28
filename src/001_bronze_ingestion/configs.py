# Databricks notebook source
import os

# ler o datalake e criar um dicionario com todas os arquivos para processamento 
def generate_ingestion_dict(folders_path):
    ingestion_tables = {}
    folders = dbutils.fs.ls(folders_path)

    for folder in folders:
        # Verifica se é uma pasta
        if folder.isDir():
            subfolder_path = folder.path
            files = dbutils.fs.ls(subfolder_path)

            for file in files:
                file_path = f"{subfolder_path.replace('dbfs:', '')}/{file.name}"
                file_name = os.path.splitext(os.path.basename(file.path))[0]

                ingestion_tables[file_name] = {
                    'file_path': file_path,
                    'file_name': file_name,
                }

    return ingestion_tables

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def ingest_bronze_and_save(file_path, table_name, database_name, input_format: str = "parquet", output_format: str = "delta"):
    """
    Ingestão genérica e salvamento em Delta Lake.
    """
    try:
        if input_format == "json":
            df = spark.read.option("multiline", "true").json(file_path)
        else:
            df = spark.read.format(input_format).load(file_path)

        df = df.withColumn("bronze_ingestion_timestamp", current_timestamp())

        df.write.mode("overwrite")\
            .option("overwriteSchema", "True")\
            .format(output_format)\
            .saveAsTable(f"{database_name}.{table_name}")

        print(f"✅ Tabela salva: {database_name}.{table_name} ({output_format})")

    except Exception as e:
        print(f"❌ Erro ao processar {table_name}: {e}")

# COMMAND ----------

print("Carregando: generate_ingestion_dict(folders_path)")
print("Carregando: ingest_bronze_and_save(file_path, table_name, database_name, input_format, output_format)")
