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
max_workers = 50

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


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     st.Name AS Region,
# MAGIC     p.Name AS Product,
# MAGIC     SUM(sod.LineTotal) AS TotalSales,
# MAGIC     COUNT(sod.SalesOrderID) AS OrderCount
# MAGIC FROM
# MAGIC     adventure_works_bronze.sales_salesorderheader soh
# MAGIC     JOIN adventure_works_bronze.sales_salesorderdetail sod ON soh.SalesOrderID = sod.SalesOrderID
# MAGIC     JOIN adventure_works_bronze.production_product p ON sod.ProductID = p.ProductID
# MAGIC     JOIN adventure_works_bronze.sales_salesterritory st ON soh.TerritoryID = st.TerritoryID
# MAGIC GROUP BY
# MAGIC     st.Name, p.Name
# MAGIC ORDER BY
# MAGIC     TotalSales DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.CustomerID,
# MAGIC     p.FullName AS CustomerName,
# MAGIC     SUM(soh.TotalDue) AS TotalSpent,
# MAGIC     COUNT(soh.SalesOrderID) AS OrderCount
# MAGIC FROM
# MAGIC     adventure_works_bronze.sales_customer c
# MAGIC     JOIN adventure_works_bronze.sales_salesorderheader soh ON c.CustomerID = soh.CustomerID
# MAGIC     JOIN (
# MAGIC         SELECT
# MAGIC             pe.BusinessEntityID,
# MAGIC             CONCAT(p.FirstName, ' ', p.LastName) AS FullName
# MAGIC         FROM
# MAGIC             adventure_works_bronze.person_person p
# MAGIC             JOIN adventure_works_bronze.person_businessentity pe ON p.BusinessEntityID = pe.BusinessEntityID
# MAGIC     ) p ON c.CustomerID = p.BusinessEntityID
# MAGIC GROUP BY
# MAGIC     c.CustomerID, p.FullName
# MAGIC ORDER BY
# MAGIC     TotalSpent DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

