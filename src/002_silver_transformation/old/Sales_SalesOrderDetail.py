# Databricks notebook source
# Enable automatic schema evolution
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# load utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType
from delta.tables import DeltaTable

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "sales_salesorderdetail"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Sales_SalesOrderDetail"
target_database = "adventure_works_prata"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderID", "SalesOrderDetailID"]

# COMMAND ----------

# Declarar schema esperado
from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DoubleType, DecimalType, TimestampType, ShortType)

expected_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),           # int NOT NULL
    StructField("SalesOrderDetailID", ShortType(), False),     # int IDENTITY(1,1) NOT NULL
    StructField("CarrierTrackingNumber", StringType(), True),    # nvarchar(25) NULL
    StructField("OrderQty", ShortType(), False),                 # smallint NOT NULL
    StructField("ProductID", IntegerType(), False),              # int NOT NULL
    StructField("SpecialOfferID", IntegerType(), False),         # int NOT NULL
    StructField("UnitPrice", DoubleType(), False),               # money NOT NULL
    StructField("UnitPriceDiscount", DoubleType(), False),       # money NOT NULL
    StructField("LineTotal", DecimalType(38, 6), False),         # numeric(38, 6) NOT NULL
    StructField("rowguid", StringType(), False),                 # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
])


# COMMAND ----------

def transform_Sales_SalesOrderDetail(SalesOrderDetail: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Sales_SalesOrderDetail

    Parâmetros:
        SalesOrderDetail (DataFrame): DataFrame contendo os dados da tabela Sales_SalesOrderDetail

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define a coluna LineTotal usando PySpark puro
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'LineTotal',
        F.when(
            F.col('UnitPrice').isNotNull() & F.col('OrderQty').isNotNull(),
            F.col('UnitPrice') * (1.0 - F.col('UnitPriceDiscount')) * F.col('OrderQty')
        ).otherwise(0.0)
    )
    
    # Define valores padrão para UnitPriceDiscount, rowguid e ModifiedDate
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'UnitPriceDiscount',
        F.when(F.col('UnitPriceDiscount').isNull(), 0.0).otherwise(F.col('UnitPriceDiscount'))
    )
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Filtra os dados conforme as restrições SQL (simulando as CHECK constraints)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('OrderQty') > 0)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('UnitPrice') >= 0.00)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('UnitPriceDiscount') >= 0.00)

    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesOrderID', 'SalesOrderDetailID').orderBy(F.col('ModifiedDate').desc())
    SalesOrderDetail = SalesOrderDetail.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesOrderDetail = SalesOrderDetail.select(
        F.col('SalesOrderID').cast(IntegerType()).alias('SalesOrderID'),
        F.col('SalesOrderDetailID').cast(ShortType()).alias('SalesOrderDetailID'),
        F.col('CarrierTrackingNumber').cast(StringType()).alias('CarrierTrackingNumber'),
        F.col('OrderQty').cast(ShortType()).alias('OrderQty'),
        F.col('ProductID').cast(IntegerType()).alias('ProductID'),
        F.col('SpecialOfferID').cast(IntegerType()).alias('SpecialOfferID'),
        F.col('UnitPrice').cast(DoubleType()).alias('UnitPrice'),
        F.col('UnitPriceDiscount').cast(DoubleType()).alias('UnitPriceDiscount'),
        F.col('LineTotal').cast(DecimalType(38,6)).alias('LineTotal'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesOrderDetail

# COMMAND ----------

transformed_df = transform_Sales_SalesOrderDetail(SalesOrderDetail = bronze_source_table)

transformed_df.display()

# COMMAND ----------

# aplicar o check do Schema
# isso vai ser uma utils
def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
    """
    Valida se o schema do DataFrame corresponde ao schema esperado.

    Parâmetros:
        df (DataFrame): O DataFrame a ser validado.
        expected_schema (StructType): O schema esperado.

    Retorna:
        bool: True se o schema corresponder, False caso contrário.
    """
    actual_schema = df.schema

    # Verifica se o número de campos corresponde
    if len(expected_schema.fields) != len(actual_schema.fields):
        return False

    # Verifica cada campo e tipo de dado
    for i, field in enumerate(actual_schema.fields):
        expected_field = expected_schema.fields[i]
        if field.name != expected_field.name or not isinstance(field.dataType, type(expected_field.dataType)):
            print(f"Discrepância encontrada na coluna: {field.name}")
            print(f"Esperado: {expected_field}, Encontrado: {field}")
            return False

    return True



# COMMAND ----------

is_schema_valid = validate_schema(transformed_df, expected_schema)

if is_schema_valid:
    print("O schema do DataFrame está correto.")
else:
    print("O schema do DataFrame está incorreto.")

    error # throw excpetion



# COMMAND ----------

def create_table_with_primary_keys(table_name: str, primary_keys: list) -> None:
    """
    Cria uma tabela Delta com as colunas de chaves primárias, caso a tabela não exista.

    Parâmetros:
        table_name (str): Nome da tabela a ser criada.
        primary_keys (list): Lista de chaves primárias a serem usadas como colunas.
    """

    # Definir esquema com base nas chaves primárias
    schema = ", ".join([f"{key} INT" for key in primary_keys])
    create_table_query = f"CREATE TABLE {table_name} ({schema}) USING DELTA"
    
    # Criar a tabela
    spark.sql(create_table_query)
    print(f"Tabela {table_name} criada com as colunas: {', '.join(primary_keys)}")


# COMMAND ----------

def upsert_silver_table(transformed_df: DataFrame, target_table: str, primary_keys: list) -> None:
    """
    Realiza o upsert (update e insert) na tabela Delta da camada prata,
    suportando a evolução do esquema e construindo dinamicamente a condição de merge.

    Parâmetros:
        transformed_df (DataFrame): DataFrame contendo os dados transformados para inserção na camada prata.
        target_table (str): Nome da tabela de destino.
        primary_keys (list): Lista de chaves primárias para o merge.

    """
    
    # Verificar se a tabela de destino existe
    if not spark.catalog.tableExists(target_table):
        # Se a tabela não existe, criá-la com as chaves primárias como colunas
        create_table_with_primary_keys(target_table, primary_keys)
    
    # Construir a condição de merge com base nas chaves primárias
    merge_condition = " AND ".join([f"s.{key} = t.{key}" for key in primary_keys])

    # Carregar a tabela Delta existente
    delta_table = DeltaTable.forName(spark, target_table)

    # Realizar o merge com suporte à evolução de esquema
    delta_table.alias("t") \
        .merge(
            transformed_df.alias("s"),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print("Excuted upsert")


# COMMAND ----------

# MAGIC %md
# MAGIC ## teste markdown

# COMMAND ----------

# Chamar a função para realizar o upsert
upsert_silver_table(transformed_df=transformed_df, target_table=target_table, primary_keys=primary_keys)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from adventure_works_prata.Sales_SalesOrderDetail

# COMMAND ----------

