# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: Customer

# COMMAND ----------

# Habilitar a evolução automática de esquemas
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

# MAGIC %run "/Workspace/AdventureWorks_Lakehouse/AdventureWorks/src/utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "sales_customer"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "sales_customer"
target_database = "adventure_works_prata"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["CustomerID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import StructType, StructField

expected_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("PersonID", IntegerType(), True),
    StructField("StoreID", IntegerType(), True),
    StructField("TerritoryID", IntegerType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("rowguid", StringType(), False),
    StructField("ModifiedDate", TimestampType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_Customer(Customer: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Customer
    '''
    Customer = Customer.withColumn(
        'AccountNumber',
        F.when(F.col('AccountNumber').isNull(), F.concat(F.lit('AW'), F.format_string('%09d', F.col('CustomerID')))).otherwise(F.col('AccountNumber'))
    )

    Customer = Customer.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )

    Customer = Customer.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )

    window_spec = Window.partitionBy('CustomerID').orderBy(F.col('ModifiedDate').desc())
    Customer = Customer.withColumn('row_num', F.row_number().over(window_spec))
    Customer = Customer.filter(F.col('row_num') == 1).drop('row_num')

    Customer = Customer.select(
        F.col('CustomerID').cast(IntegerType()).alias('CustomerID'),
        F.col('PersonID').cast(IntegerType()).alias('PersonID'),
        F.col('StoreID').cast(IntegerType()).alias('StoreID'),
        F.col('TerritoryID').cast(IntegerType()).alias('TerritoryID'),
        F.col('AccountNumber').cast(StringType()).alias('AccountNumber'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return Customer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Customer(Customer=bronze_source_table)

transformed_df.count()
transformed_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação do Schema

# COMMAND ----------

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

_upsert_silver_table(transformed_df, target_table, primary_keys, not_matched_by_source_action="DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar os Dados na Tabela Prata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adventure_works_prata.sales_customer limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from adventure_works_prata.sales_customer
