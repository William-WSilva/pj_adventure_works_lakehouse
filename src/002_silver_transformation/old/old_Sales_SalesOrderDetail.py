# Databricks notebook source
# MAGIC %sql
# MAGIC -- Comando para inserir múltiplas linhas na tabela SalesOrderDetail
# MAGIC INSERT INTO adventure_works_bronze.Sales_SalesOrderDetail (
# MAGIC     SalesOrderID,
# MAGIC     SalesOrderDetailID,
# MAGIC     CarrierTrackingNumber,
# MAGIC     OrderQty,
# MAGIC     ProductID,
# MAGIC     SpecialOfferID,
# MAGIC     UnitPrice,
# MAGIC     UnitPriceDiscount,
# MAGIC     LineTotal,
# MAGIC     rowguid,
# MAGIC     ModifiedDate
# MAGIC )
# MAGIC VALUES
# MAGIC     (43659, 3, '4911-403C-98', 1, 778, 1, 2024.994, 0, 2024.994, '475cf8c6-49f6-486e-b0ad-afc6a50cdd2f', '2011-05-31T00:00:00.000+00:00'),
# MAGIC     (43659, 10, '4911-403C-98', 6, 709, 1, 5.7, 0, 34.2, 'ac769034-3c2f-495c-a5a7-3b71cdb25d4e', '2011-05-31T00:00:00.000+00:00'),
# MAGIC     (43661, 15, '4E0A-4F89-AE', 1, 745, 1, 809.76, 0, 809.76, 'ede1759e-6733-4c7b-a43f-dc6f48002d8a', '2011-05-31T00:00:00.000+00:00'),
# MAGIC     (43661, 17, '4E0A-4F89-AE', 2, 747, 1, 714.7043, 0, 1429.4086, 'b136852e-24c9-4006-8048-b14aefe6c337', '2011-05-31T00:00:00.000+00:00');
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

table = "Sales_SalesOrderDetail"
database = "adventure_works_bronze"
tabela_bronze = spark.read.table(f"{database}.{table}")




# COMMAND ----------

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType

def logic_Sales_SalesOrderDetail(tabela: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Sales_SalesOrderDetail

    Parâmetros:
        tabela (DataFrame): DataFrame contendo os dados da tabela Sales_SalesOrderDetail

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define a coluna LineTotal usando PySpark puro
    tabela = tabela.withColumn(
        'LineTotal',
        F.when(
            F.col('UnitPrice').isNotNull() & F.col('OrderQty').isNotNull(),
            F.col('UnitPrice') * (1.0 - F.col('UnitPriceDiscount')) * F.col('OrderQty')
        ).otherwise(0.0)
    )
    
    # Define valores padrão para UnitPriceDiscount, rowguid e ModifiedDate
    tabela = tabela.withColumn(
        'UnitPriceDiscount',
        F.when(F.col('UnitPriceDiscount').isNull(), 0.0).otherwise(F.col('UnitPriceDiscount'))
    )
    tabela = tabela.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    tabela = tabela.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Filtra os dados conforme as restrições SQL (simulando as CHECK constraints)
    tabela = tabela.filter(F.col('OrderQty') > 0)
    tabela = tabela.filter(F.col('UnitPrice') >= 0.00)
    tabela = tabela.filter(F.col('UnitPriceDiscount') >= 0.00)

    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesOrderID', 'SalesOrderDetailID').orderBy(F.col('ModifiedDate').desc())
    tabela = tabela.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    tabela = tabela.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    tabela = tabela.select(
        F.col('SalesOrderID').cast(IntegerType()).alias('SalesOrderID'),
        F.col('SalesOrderDetailID').cast(IntegerType()).alias('SalesOrderDetailID'),
        F.col('CarrierTrackingNumber').cast(StringType()).alias('CarrierTrackingNumber'),
        F.col('OrderQty').cast(IntegerType()).alias('OrderQty'),
        F.col('ProductID').cast(IntegerType()).alias('ProductID'),
        F.col('SpecialOfferID').cast(IntegerType()).alias('SpecialOfferID'),
        F.col('UnitPrice').cast(DoubleType()).alias('UnitPrice'),
        F.col('UnitPriceDiscount').cast(DoubleType()).alias('UnitPriceDiscount'),
        F.col('LineTotal').cast(DoubleType()).alias('LineTotal'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return tabela


# COMMAND ----------

tabela_bronze_filtrada = tabela_bronze.where(" SalesOrderID = 43659")

output = logic_Sales_SalesOrderDetail(tabela_bronze)

output.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

def criar_tabela_com_meu_nome(tabela_input):
    
    nova_tabela = tabela_input.withColumn("coluna_nova", F.lit('Bernardo'))
    nova_tabela = nova_tabela.withColumn("coluna_nova_2", lit('Bernardo'))

    return nova_tabela


# COMMAND ----------

criar_tabela_com_meu_nome(df).display()

# COMMAND ----------

