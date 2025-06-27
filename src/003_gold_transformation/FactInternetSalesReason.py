# Databricks notebook source

# Databricks notebook source
# MAGIC %run "/Workspace/AdventureWorks_Lakehouse/AdventureWorks/src/utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, ShortType, TimestampType, StringType,
    DecimalType, StructType, StructField
)

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "FactInternetSalesReason"
target_database = "adventure_works_ouro"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderNumber", "SalesOrderLineNumber", "SalesReasonKey"]

# Informações da Tabela Fonte
sales_order_header_df = spark.read.table("adventure_works_prata.sales_salesorderheader")
sales_order_detail_df = spark.read.table("adventure_works_prata.sales_salesorderdetail")
sales_reason_df = spark.read.table("adventure_works_prata.sales_salesreason")
sales_order_reason_df = spark.read.table("adventure_works_prata.sales_salesorderheadersalesreason")

# COMMAND ----------

expected_schema = StructType([
    StructField("SalesOrderNumber", StringType(), False),
    StructField("SalesOrderLineNumber", IntegerType(), False),
    StructField("SalesReasonKey", IntegerType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `FactInternetSalesReason`

# COMMAND ----------

def transform_fact_internet_sales_reason(
    sales_order_header_df: DataFrame,
    sales_order_detail_df: DataFrame,
    sales_reason_df: DataFrame,
    sales_order_reason_df: DataFrame
) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: FactInternetSalesReason

    Parâmetros:
        sales_order_header_df (DataFrame): DataFrame da tabela SalesOrderHeader.
        sales_order_detail_df (DataFrame): DataFrame da tabela SalesOrderDetail.
        sales_reason_df (DataFrame): DataFrame da tabela SalesReason.
        sales_order_reason_df (DataFrame): DataFrame da tabela SalesOrderHeaderSalesReason.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela SalesOrderDetail com base nas chaves primárias e ordenada pela coluna 'ModifiedDate'
    window_spec_detail = Window.partitionBy("SalesOrderID", "SalesOrderDetailID").orderBy(F.col("ModifiedDate").desc())
    sales_order_detail_df = sales_order_detail_df.withColumn("row_num", F.row_number().over(window_spec_detail))
    sales_order_detail_df = sales_order_detail_df.filter(F.col("row_num") == 1).drop("row_num")

    # Realizando os joins após a deduplicação
    fact_internet_sales_reason = (
        sales_order_header_df.alias("soh")
            .join(sales_order_detail_df.alias("sod"), "SalesOrderID", "inner")
            .join(sales_order_reason_df.alias("soh_sr"), "SalesOrderID", "inner")
            .join(sales_reason_df.alias("sr"), "SalesReasonID", "inner")
    )

    # Selecionando colunas e deduplicando a tabela final
    fact_internet_sales_reason = fact_internet_sales_reason.select(
        F.col("soh.SalesOrderNumber").alias("SalesOrderNumber"),
        F.col("sod.SalesOrderDetailID").alias("SalesOrderLineNumber"),
        F.col("sr.SalesReasonID").alias("SalesReasonKey"),
        F.col("soh.ModifiedDate").alias("ModifiedDate")  # Exemplo: deduplicação com base nesta coluna
    )

    # Deduplicação da tabela final `FactInternetSalesReason`
    window_spec_final = Window.partitionBy("SalesOrderNumber", "SalesOrderLineNumber", "SalesReasonKey").orderBy(F.col("ModifiedDate").desc())
    fact_internet_sales_reason = fact_internet_sales_reason.withColumn("row_num", F.row_number().over(window_spec_final))
    fact_internet_sales_reason = fact_internet_sales_reason.filter(F.col("row_num") == 1).drop("row_num", "ModifiedDate")

    return fact_internet_sales_reason

# COMMAND ----------

# Transformando os dados
transformed_df = transform_fact_internet_sales_reason(
    sales_order_header_df=sales_order_header_df,
    sales_order_detail_df=sales_order_detail_df,
    sales_reason_df=sales_reason_df,
    sales_order_reason_df=sales_order_reason_df
)

# Verificação da contagem e do schema
transformed_df.count()  # Verifica o número de linhas
transformed_df.printSchema()  # Verifica o schema do DataFrame

# COMMAND ----------

# Validação do Schema
is_schema_valid = _validate_schema(transformed_df, expected_schema)

if is_schema_valid:
    print("O schema do DataFrame está correto.")
else:
    print("O schema do DataFrame está incorreto.")
    raise Exception("Schema validation failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita da Tabela `FactInternetSalesReason` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
