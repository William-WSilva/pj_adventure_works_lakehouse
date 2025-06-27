# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: Customer
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Habilitar a evolução automática de esquemas
# MAGIC spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %run "/Workspace/Users/roseaneinacio@nw5y.onmicrosoft.com/ws_pj_adventure_works_lakehouse/src/utils/common_functions"
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC from pyspark.sql import DataFrame, Window
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.types import (
# MAGIC     IntegerType, StringType, TimestampType
# MAGIC )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Informações da Tabela Fonte
# MAGIC source_table = "sales_customer"
# MAGIC source_database = "adventure_works_bronze"
# MAGIC bronze_source_table = spark.read.table(f"{source_database}.{source_table}")
# MAGIC 
# MAGIC # Informações da Tabela Destino (target)
# MAGIC target_table_name = "sales_customer"
# MAGIC target_database = silver_db
# MAGIC target_table = f"{target_database}.{target_table_name}"
# MAGIC 
# MAGIC primary_keys = ["CustomerID"]
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Declarar Schema Esperado
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC from pyspark.sql.types import (StructType, StructField,
# MAGIC         IntegerType, StringType, TimestampType)
# MAGIC 
# MAGIC expected_schema = StructType([
# MAGIC     StructField("CustomerID", IntegerType(), False),             # int IDENTITY(1,1) NOT NULL
# MAGIC     StructField("PersonID", IntegerType(), True),                # int NULL
# MAGIC     StructField("StoreID", IntegerType(), True),                 # int NULL
# MAGIC     StructField("TerritoryID", IntegerType(), True),             # int NULL
# MAGIC     StructField("AccountNumber", StringType(), True),            # Calculado: 'AW' + LeadingZeros(CustomerID)
# MAGIC     StructField("rowguid", StringType(), False),                 # uniqueidentifier NOT NULL
# MAGIC     StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
# MAGIC ])
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Função de Transformação
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC def transform_Customer(Customer: DataFrame) -> DataFrame:
# MAGIC     '''
# MAGIC     Transformação da tabela: Customer
# MAGIC 
# MAGIC     Parâmetros:
# MAGIC         Customer (DataFrame): DataFrame contendo os dados da tabela Customer
# MAGIC 
# MAGIC     Retorna:
# MAGIC         DataFrame: O DataFrame resultante após a transformação e deduplicação.
# MAGIC     '''
# MAGIC     
# MAGIC     # Calcula o AccountNumber baseado no CustomerID
# MAGIC     Customer = Customer.withColumn(
# MAGIC         'AccountNumber',
# MAGIC         F.when(F.col('AccountNumber').isNull(), F.concat(F.lit('AW'), F.format_string('%09d', F.col('CustomerID')))).otherwise(F.col('AccountNumber'))
# MAGIC     )
# MAGIC     
# MAGIC     # Define valores padrão para campos que podem ser nulos
# MAGIC     Customer = Customer.withColumn(
# MAGIC         'rowguid',
# MAGIC         F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
# MAGIC     )
# MAGIC     
# MAGIC     Customer = Customer.withColumn(
# MAGIC         'ModifiedDate',
# MAGIC         F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
# MAGIC     )
# MAGIC     
# MAGIC     # Define a função de janela para deduplicar com base nas chaves primárias
# MAGIC     window_spec = Window.partitionBy('CustomerID').orderBy(F.col('ModifiedDate').desc())
# MAGIC     Customer = Customer.withColumn('row_num', F.row_number().over(window_spec))
# MAGIC 
# MAGIC     # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
# MAGIC     Customer = Customer.filter(F.col('row_num') == 1).drop('row_num')
# MAGIC 
# MAGIC     # Seleção final com CAST explícito dos tipos de dados
# MAGIC     Customer = Customer.select(
# MAGIC         F.col('CustomerID').cast(IntegerType()).alias('CustomerID'),
# MAGIC         F.col('PersonID').cast(IntegerType()).alias('PersonID'),
# MAGIC         F.col('StoreID').cast(IntegerType()).alias('StoreID'),
# MAGIC         F.col('TerritoryID').cast(IntegerType()).alias('TerritoryID'),
# MAGIC         F.col('AccountNumber').cast(StringType()).alias('AccountNumber'),
# MAGIC         F.col('rowguid').cast(StringType()).alias('rowguid'),
# MAGIC         F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
# MAGIC     )
# MAGIC 
# MAGIC     return Customer
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Aplicar Transformação
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC transformed_df = transform_Customer(Customer = bronze_source_table)
# MAGIC 
# MAGIC transformed_df.count() #quick way to check rows
# MAGIC transformed_df.printSchema() #quick way to check schema
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Verificação do Schema
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Verificação do Schema
# MAGIC is_schema_valid = _validate_schema(transformed_df, expected_schema)
# MAGIC 
# MAGIC if is_schema_valid:
# MAGIC     print("O schema do DataFrame está correto.")
# MAGIC else:
# MAGIC     print("O schema do DataFrame está incorreto.")
# MAGIC     raise Exception("Schema validation failed.")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Executar o Upsert na Tabela Prata
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Chamar a função para realizar o upsert
# MAGIC _upsert_silver_table(transformed_df, target_table, primary_keys, not_matched_by_source_action="DELETE")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Verificar os Dados na Tabela Prata
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %sql 
# MAGIC select * from adventure_works_prata.sales_customer limit 10
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %sql 
# MAGIC select count(*) from adventure_works_prata.sales_customer