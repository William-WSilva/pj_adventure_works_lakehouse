# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: sales_salesorderheadersalesreason

# COMMAND ----------

# Habilitar a evolução automática de esquemas
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/roseaneinacio@nw5y.onmicrosoft.com/ws_pj_adventure_works_lakehouse/src/utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
table_name = "sales_salesorderheadersalesreason"
source_table = table_name
source_database = bronze_db
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = table_name
target_database = silver_db
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderID", "SalesReasonID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, TimestampType)

expected_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),    # int NOT NULL
    StructField("SalesReasonID", IntegerType(), False),   # int NOT NULL
    StructField("ModifiedDate", TimestampType(), False)   # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_SalesOrderHeaderSalesReason(SalesOrderHeaderSalesReason: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: SalesOrderHeaderSalesReason

    Parâmetros:
        SalesOrderHeaderSalesReason (DataFrame): DataFrame contendo os dados da tabela SalesOrderHeaderSalesReason

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define valores padrão para campos que podem ser nulos
    SalesOrderHeaderSalesReason = SalesOrderHeaderSalesReason.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesOrderID', 'SalesReasonID').orderBy(F.col('ModifiedDate').desc())
    SalesOrderHeaderSalesReason = SalesOrderHeaderSalesReason.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesOrderHeaderSalesReason = SalesOrderHeaderSalesReason.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesOrderHeaderSalesReason = SalesOrderHeaderSalesReason.select(
        F.col('SalesOrderID').cast(IntegerType()).alias('SalesOrderID'),
        F.col('SalesReasonID').cast(IntegerType()).alias('SalesReasonID'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesOrderHeaderSalesReason

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_SalesOrderHeaderSalesReason(SalesOrderHeaderSalesReason = bronze_source_table)

transformed_df.count() #quick way to check rows
transformed_df.printSchema() #quick way to check schema

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
# MAGIC select * from hive_metastore.adventure_works_prata.sales_salesorderheadersalesreason limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from hive_metastore.adventure_works_prata.sales_salesorderheadersalesreason