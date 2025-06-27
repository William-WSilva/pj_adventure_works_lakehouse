# Databricks notebook source
# MAGIC %md
# MAGIC ## Parâmetro da Tabela
# MAGIC - Este notebook recebe um parâmetro `TABLE_NAME` para definir qual tabela será processada.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 Configuração das Credenciais do Azure SQL Database

# COMMAND ----------

JDBC_HOSTNAME = "sqldb-study-lakehouse-server.database.windows.net"
JDBC_DATABASE = "AdventureWorks2022"
JDBC_PORT = "1433"
JDBC_USERNAME = "SEU_USUARIO_AQUI"  # Alternativamente, use um Databricks Secret
JDBC_PASSWORD = "SUA_SENHA_AQUI"  # Alternativamente, use um Databricks Secret

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 Importação das Bibliotecas

# COMMAND ----------

from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder.appName("BronzeLayerAzureSQL").getOrCreate()

# Construir a URL JDBC
jdbc_url = f"jdbc:sqlserver://{JDBC_HOSTNAME}:{JDBC_PORT};database={JDBC_DATABASE};encrypt=true;trustServerCertificate=false;loginTimeout=30;"

# Propriedades de conexão JDBC
connection_properties = {
    "user": JDBC_USERNAME,
    "password": JDBC_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Criar Database Bronze no Databricks
spark.sql("CREATE DATABASE IF NOT EXISTS adventure_works_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 Definição das Tabelas e Esquemas

# COMMAND ----------

# Dicionário com as tabelas selecionadas (ordenadas alfabeticamente) com os SCHEMAS corretos
dict_tables = {
    "person_address": "Person.Address",
    "person_countryregion": "Person.CountryRegion",
    "person_emailaddress": "Person.EmailAddress",
    "person_person": "Person.Person",
    "person_personphone": "Person.PersonPhone",
    "person_stateprovince": "Person.StateProvince",
    "production_product": "Production.Product",
    "production_productcategory": "Production.ProductCategory",
    "production_productdescription": "Production.ProductDescription",
    "production_productmodel": "Production.ProductModel",
    "production_productsubcategory": "Production.ProductSubcategory",
    "sales_currency": "Sales.Currency",
    "sales_customer": "Sales.Customer",
    "sales_salesorderdetail": "Sales.SalesOrderDetail",
    "sales_salesorderheader": "Sales.SalesOrderHeader",
    "sales_salesreason": "Sales.SalesReason",
    "sales_salesterritory": "Sales.SalesTerritory",
    "sales_specialoffer": "Sales.SpecialOffer"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 Captura do Parâmetro da Tabela

# COMMAND ----------

# Criar widget para entrada do nome da tabela, caso ainda não exista
dbutils.widgets.text("TABLE_NAME", "sales_salesorderheader", "Escolha a tabela")

# Ler o parâmetro passado pelo usuário
TABLE_NAME = dbutils.widgets.get("TABLE_NAME")

# Verificar se a tabela existe no dicionário
if TABLE_NAME not in dict_tables:
    raise ValueError(f"Tabela '{TABLE_NAME}' não encontrada no dicionário de tabelas disponíveis.")

SCHEMA_NAME = dict_tables[TABLE_NAME]
print(f"Extraindo dados da tabela: {SCHEMA_NAME} -> {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 Leitura da Tabela Específica

# COMMAND ----------

# Lendo a tabela específica do SQL Server
df = spark.read.jdbc(url=jdbc_url, table=f"{SCHEMA_NAME}", properties=connection_properties)

# Exibir amostra dos dados
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 Salvando os Dados na Camada Bronze

# COMMAND ----------

# Escrever como Delta Table na camada Bronze
df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"adventure_works_bronze.{TABLE_NAME}")

print(f"Dados da tabela '{TABLE_NAME}' salvos na camada Bronze como adventure_works_bronze.{TABLE_NAME}")
