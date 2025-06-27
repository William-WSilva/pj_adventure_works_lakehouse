# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: production_productcosthistory

# COMMAND ----------

# Habilitar a evolução automática de esquemas
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/roseaneinacio@nw5y.onmicrosoft.com/ws_pj_adventure_works_lakehouse/src/utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, TimestampType, DecimalType, StructType, StructField
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "production_productcosthistory"
source_database = "hive_metastore.adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Production_ProductCostHistory"
target_database = "hive_metastore.adventure_works_prata"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["ProductID", "StartDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

expected_schema = StructType([
    StructField("ProductID", IntegerType(), False),         # int NOT NULL
    StructField("StartDate", TimestampType(), False),       # datetime NOT NULL
    StructField("EndDate", TimestampType(), True),          # datetime NULL
    StructField("StandardCost", DecimalType(19, 4), False), # money NOT NULL
    StructField("ModifiedDate", TimestampType(), False)     # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_ProductCostHistory(ProductCostHistory: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: ProductCostHistory

    Parâmetros:
        ProductCostHistory (DataFrame): DataFrame contendo os dados da tabela ProductCostHistory

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Aplicar regras de integridade e preencher campos nulos
    ProductCostHistory = ProductCostHistory.withColumn(
        'EndDate', F.when(F.col('EndDate') < F.col('StartDate'), None).otherwise(F.col('EndDate'))
    )

    # Filtrar linhas com StandardCost >= 0
    ProductCostHistory = ProductCostHistory.filter(F.col('StandardCost') >= 0.00)

    # Gerar valores de ModifiedDate para linhas onde não existe
    ProductCostHistory = ProductCostHistory.withColumn(
        'ModifiedDate', 
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )

    # Deduplicação utilizando uma função de janela
    window_spec = Window.partitionBy('ProductID', 'StartDate').orderBy(F.col('ModifiedDate').desc())
    ProductCostHistory = ProductCostHistory.withColumn('row_num', F.row_number().over(window_spec))

    # Filtrar para manter apenas a linha mais recente
    ProductCostHistory = ProductCostHistory.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    ProductCostHistory = ProductCostHistory.select(
        F.col('ProductID').cast(IntegerType()).alias('ProductID'),
        F.col('StartDate').cast(TimestampType()).alias('StartDate'),
        F.col('EndDate').cast(TimestampType()).alias('EndDate'),
        F.col('StandardCost').cast(DecimalType(19, 4)).alias('StandardCost'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return ProductCostHistory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_ProductCostHistory(ProductCostHistory = bronze_source_table)

transformed_df.count() # quick way to check rows
transformed_df.printSchema() # quick way to check schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação do Schema

# COMMAND ----------

# Verificação do Schema
is_schema_valid = _validate_schema(transformed_df, expected_schema)

if is_schema_valid:
    print("O schema do DataFrame está correto.")
else:
    print("O schema do DataFrame está incorreto.")
    raise Exception("Schema validation failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar o Upsert na Tabela Prata

# COMMAND ----------

# Chamar a função para realizar o upsert
_upsert_silver_table(transformed_df, target_table, primary_keys, not_matched_by_source_action="DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar os Dados na Tabela Prata

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from adventure_works_prata.Production_ProductCostHistory limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_prata.Production_ProductCostHistory