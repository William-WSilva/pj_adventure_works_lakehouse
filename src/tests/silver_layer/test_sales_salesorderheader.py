# Databricks notebook source
# MAGIC %md
# MAGIC ##Teste da logica de prata-> sales_salesorderheader

# COMMAND ----------

# MAGIC %run "/Workspace/Users/roseaneinacio@nw5y.onmicrosoft.com/ws_pj_adventure_works_lakehouse/src/utils/test_functions"

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DoubleType, DecimalType, TimestampType, ShortType)

expected_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),           # int IDENTITY(1,1) NOT NULL
    StructField("RevisionNumber", ShortType(), False),           # tinyint NOT NULL
    StructField("OrderDate", TimestampType(), False),            # datetime NOT NULL
    StructField("DueDate", TimestampType(), False),              # datetime NOT NULL
    StructField("ShipDate", TimestampType(), True),              # datetime NULL
    StructField("Status", ShortType(), False),                   # tinyint NOT NULL
    StructField("OnlineOrderFlag", IntegerType(), False),        # Flag NOT NULL 
    StructField("SalesOrderNumber", StringType(), True),         # nvarchar(23) NOT NULL
    StructField("PurchaseOrderNumber", StringType(), True),      # OrderNumber NULL
    StructField("AccountNumber", StringType(), True),            # AccountNumber NULL
    StructField("CustomerID", IntegerType(), False),             # int NOT NULL
    StructField("SalesPersonID", IntegerType(), True),           # int NULL
    StructField("TerritoryID", IntegerType(), True),             # int NULL
    StructField("BillToAddressID", IntegerType(), False),        # int NOT NULL
    StructField("ShipToAddressID", IntegerType(), False),        # int NOT NULL
    StructField("ShipMethodID", IntegerType(), False),           # int NOT NULL
    StructField("CreditCardID", IntegerType(), True),            # int NULL
    StructField("CreditCardApprovalCode", StringType(), True),   # varchar(15) NULL
    StructField("CurrencyRateID", IntegerType(), True),          # int NULL
    StructField("SubTotal", DecimalType(19, 4), False),          # money NOT NULL
    StructField("TaxAmt", DecimalType(19, 4), False),            # money NOT NULL
    StructField("Freight", DecimalType(19, 4), False),           # money NOT NULL
    StructField("TotalDue", DecimalType(38, 6), True),           # numeric(38, 6) NOT NULL
    StructField("Comment", StringType(), True),                  # nvarchar(128) NULL
    StructField("rowguid", StringType(), False),                 # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
])

# COMMAND ----------

# Célula de Input
# Conteúdo CSV como uma string, usando | como delimitador
csv_input = """SalesOrderID|RevisionNumber|OrderDate|DueDate|ShipDate|Status|OnlineOrderFlag|SalesOrderNumber|PurchaseOrderNumber|AccountNumber|CustomerID|SalesPersonID|TerritoryID|BillToAddressID|ShipToAddressID|ShipMethodID|CreditCardID|CreditCardApprovalCode|CurrencyRateID|SubTotal|TaxAmt|Freight|TotalDue|Comment|rowguid|ModifiedDate
43659|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43659|PO522145787|10-4020-000676|29825|279|5|985|985|5|16281|105041Vi84182|NULL|20565.6206|1971.5149|616.0984|23153.2339|NULL|79b65321-39ca-4115-9cba-8fe0903e12e6|2011-06-07 00:00:00.000
43660|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43660|PO18850127500|10-4020-000117|29672|279|5|921|921|5|5618|115213Vi29411|NULL|1294.2529|124.2483|38.8276|1457.3288|NULL|738dc42d-d03b-48a1-9822-f95a67ea7389|2011-06-07 00:00:00.000
43661|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43661|PO18473189620|10-4020-000442|29734|282|6|517|517|5|1346|85274Vi6854|4|32726.4786|3153.7696|985.553|36865.8012|NULL|d91b9131-18a4-4a11-bc3a-90b6f53e9d74|2011-06-07 00:00:00.000
43662|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43662|PO18444174044|10-4020-000227|29994|282|6|482|482|5|10456|125295Vi53935|4|28832.5289|2775.1646|867.2389|32474.9324|NULL|4a1ecfc0-cc3a-4740-b028-1c50bb48711c|2011-06-07 00:00:00.000
43663|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43663|PO18009186470|10-4020-000510|29565|276|4|1073|1073|5|4322|45303Vi22691|NULL|419.4589|40.2681|12.5838|472.3108|NULL|9b1e7a40-6ae0-4ad3-811c-a64951857c4b|2011-06-07 00:00:00.000
43664|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43664|PO16617121983|10-4020-000397|29898|280|1|876|876|5|806|95555Vi4081|NULL|24432.6088|2344.9921|732.81|27510.4109|NULL|22a8a5da-8c22-42ad-9241-839489b6ef0d|2011-06-07 00:00:00.000
43665|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43665|PO16588191572|10-4020-000146|29580|283|1|849|849|5|15232|35568Vi78804|NULL|14352.7713|1375.9427|429.9821|16158.6961|NULL|5602c304-853c-43d7-9e79-76e320d476cf|2011-06-07 00:00:00.000
43666|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43666|PO16008173883|10-4020-000511|30052|276|4|1074|1074|5|13349|105623Vi69217|NULL|5056.4896|486.3747|151.9921|5694.8564|NULL|e2a90057-1366-4487-8a7e-8085845ff770|2011-06-07 00:00:00.000
43667|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43667|PO15428132599|10-4020-000646|29974|277|3|629|629|5|10370|55680Vi53503|NULL|6107.082|586.1203|183.1626|6876.3649|NULL|86d5237d-432d-4b21-8abc-671942f5789d|2011-06-07 00:00:00.000
43668|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43668|PO14732180295|10-4020-000514|29614|282|6|529|529|5|1566|85817Vi8045|4|35944.1562|3461.7654|1081.8017|40487.7233|NULL|281cc355-d538-494e-9b44-461b36a826c6|2011-06-07 00:00:00.000
43668|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43668|PO14732180295|10-4020-000514|29614|282|6|529|529|5|1566|85817Vi8045|4|35944.1562|3461.7654|1081.8017|40487.7233|NULL|281cc355-d538-494e-9b44-461b36a826c6|2011-06-07 00:00:00.000
43668|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43668|PO14732180295|10-4020-000514|29614|282|6|529|529|5|1566|85817Vi8045|4|35944.1562|3461.7654|1081.8017|40487.7233|NULL|281cc355-d538-494e-9b44-461b36a826c6|2011-06-07 00:00:00.000
43668|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43668|PO14732180295|10-4020-000514|29614|282|6|529|529|5|1566|85817Vi8045|4|35944.1562|3461.7654|1081.8017|40487.7233|NULL|281cc355-d538-494e-9b44-461b36a826c6|2011-06-07 00:00:00.000
"""


# Criar um RDD a partir do conteúdo CSV
rdd_input = spark.sparkContext.parallelize(csv_input.splitlines())

# Converter o RDD para um DataFrame usando o delimitador |
input_df = spark.read.csv(rdd_input, sep="|", header=True, inferSchema=True)

# Exibir o DataFrame de entrada
display(input_df)


# COMMAND ----------

# Célula de Expected Output
csv_expected = """SalesOrderID|RevisionNumber|OrderDate|DueDate|ShipDate|Status|OnlineOrderFlag|SalesOrderNumber|PurchaseOrderNumber|AccountNumber|CustomerID|SalesPersonID|TerritoryID|BillToAddressID|ShipToAddressID|ShipMethodID|CreditCardID|CreditCardApprovalCode|CurrencyRateID|SubTotal|TaxAmt|Freight|TotalDue|Comment|rowguid|ModifiedDate
43659|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43659|PO522145787|10-4020-000676|29825|279|5|985|985|5|16281|105041Vi84182|NULL|20565.6206|1971.5149|616.0984|23153.2339|NULL|79b65321-39ca-4115-9cba-8fe0903e12e6|2011-06-07 00:00:00.000
43660|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43660|PO18850127500|10-4020-000117|29672|279|5|921|921|5|5618|115213Vi29411|NULL|1294.2529|124.2483|38.8276|1457.3288|NULL|738dc42d-d03b-48a1-9822-f95a67ea7389|2011-06-07 00:00:00.000
43661|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43661|PO18473189620|10-4020-000442|29734|282|6|517|517|5|1346|85274Vi6854|4|32726.4786|3153.7696|985.553|36865.8012|NULL|d91b9131-18a4-4a11-bc3a-90b6f53e9d74|2011-06-07 00:00:00.000
43662|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43662|PO18444174044|10-4020-000227|29994|282|6|482|482|5|10456|125295Vi53935|4|28832.5289|2775.1646|867.2389|32474.9324|NULL|4a1ecfc0-cc3a-4740-b028-1c50bb48711c|2011-06-07 00:00:00.000
43663|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43663|PO18009186470|10-4020-000510|29565|276|4|1073|1073|5|4322|45303Vi22691|NULL|419.4589|40.2681|12.5838|472.3108|NULL|9b1e7a40-6ae0-4ad3-811c-a64951857c4b|2011-06-07 00:00:00.000
43664|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43664|PO16617121983|10-4020-000397|29898|280|1|876|876|5|806|95555Vi4081|NULL|24432.6088|2344.9921|732.81|27510.4109|NULL|22a8a5da-8c22-42ad-9241-839489b6ef0d|2011-06-07 00:00:00.000
43665|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43665|PO16588191572|10-4020-000146|29580|283|1|849|849|5|15232|35568Vi78804|NULL|14352.7713|1375.9427|429.9821|16158.6961|NULL|5602c304-853c-43d7-9e79-76e320d476cf|2011-06-07 00:00:00.000
43666|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43666|PO16008173883|10-4020-000511|30052|276|4|1074|1074|5|13349|105623Vi69217|NULL|5056.4896|486.3747|151.9921|5694.8564|NULL|e2a90057-1366-4487-8a7e-8085845ff770|2011-06-07 00:00:00.000
43667|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43667|PO15428132599|10-4020-000646|29974|277|3|629|629|5|10370|55680Vi53503|NULL|6107.082|586.1203|183.1626|6876.3649|NULL|86d5237d-432d-4b21-8abc-671942f5789d|2011-06-07 00:00:00.000
43668|8|2011-05-31 00:00:00.000|2011-06-12 00:00:00.000|2011-06-07 00:00:00.000|5|0|SO43668|PO14732180295|10-4020-000514|29614|282|6|529|529|5|1566|85817Vi8045|4|35944.1562|3461.7654|1081.8017|40487.7233|NULL|281cc355-d538-494e-9b44-461b36a826c6|2011-06-07 
"""

# Criar um RDD a partir do conteúdo CSV esperado
rdd_expected = spark.sparkContext.parallelize(csv_expected.splitlines())

# Converter o RDD para um DataFrame usando o delimitador |
expected_df = spark.read.csv(rdd_expected, sep="|", header=True, schema=expected_schema)

# Exibir o DataFrame esperado
display(expected_df)


# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, ShortType, TimestampType, StringType,
    DecimalType
)

def transform_Sales_SalesOrderHeader(SalesOrderHeader: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Sales_SalesOrderHeader

    Parâmetros:
        SalesOrderHeader (DataFrame): DataFrame contendo os dados da tabela Sales_SalesOrderHeader

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define valores padrão para campos que podem ser nulos
    SalesOrderHeader = SalesOrderHeader.withColumn(
        'TotalDue',
        F.when(F.col('SubTotal').isNotNull() & F.col('TaxAmt').isNotNull() & F.col('Freight').isNotNull(),
               F.col('SubTotal') + F.col('TaxAmt') + F.col('Freight')).otherwise(0.0)
    )
    
    SalesOrderHeader = SalesOrderHeader.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    SalesOrderHeader = SalesOrderHeader.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Aplicando checks de integridade
    SalesOrderHeader = SalesOrderHeader.filter(F.col('SubTotal') >= 0.00)
    SalesOrderHeader = SalesOrderHeader.filter(F.col('TaxAmt') >= 0.00)
    SalesOrderHeader = SalesOrderHeader.filter(F.col('Freight') >= 0.00)
    SalesOrderHeader = SalesOrderHeader.filter((F.col('Status') >= 0) & (F.col('Status') <= 8))
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesOrderID').orderBy(F.col('ModifiedDate').desc())
    SalesOrderHeader = SalesOrderHeader.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesOrderHeader = SalesOrderHeader.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesOrderHeader = SalesOrderHeader.select(
        F.col('SalesOrderID').cast(IntegerType()).alias('SalesOrderID'),
        F.col('RevisionNumber').cast(ShortType()).alias('RevisionNumber'),
        F.col('OrderDate').cast(TimestampType()).alias('OrderDate'),
        F.col('DueDate').cast(TimestampType()).alias('DueDate'),
        F.col('ShipDate').cast(TimestampType()).alias('ShipDate'),
        F.col('Status').cast(ShortType()).alias('Status'),
        F.col('OnlineOrderFlag').cast(IntegerType()).alias('OnlineOrderFlag'),
        F.col('SalesOrderNumber').cast(StringType()).alias('SalesOrderNumber'),
        F.col('PurchaseOrderNumber').cast(StringType()).alias('PurchaseOrderNumber'),
        F.col('AccountNumber').cast(StringType()).alias('AccountNumber'),
        F.col('CustomerID').cast(IntegerType()).alias('CustomerID'),
        F.col('SalesPersonID').cast(IntegerType()).alias('SalesPersonID'),
        F.col('TerritoryID').cast(IntegerType()).alias('TerritoryID'),
        F.col('BillToAddressID').cast(IntegerType()).alias('BillToAddressID'),
        F.col('ShipToAddressID').cast(IntegerType()).alias('ShipToAddressID'),
        F.col('ShipMethodID').cast(IntegerType()).alias('ShipMethodID'),
        F.col('CreditCardID').cast(IntegerType()).alias('CreditCardID'),
        F.col('CreditCardApprovalCode').cast(StringType()).alias('CreditCardApprovalCode'),
        F.col('CurrencyRateID').cast(IntegerType()).alias('CurrencyRateID'),
        F.col('SubTotal').cast(DecimalType(19,4)).alias('SubTotal'),
        F.col('TaxAmt').cast(DecimalType(19,4)).alias('TaxAmt'),
        F.col('Freight').cast(DecimalType(19,4)).alias('Freight'),
        F.col('TotalDue').cast(DecimalType(38,6)).alias('TotalDue'),
        F.col('Comment').cast(StringType()).alias('Comment'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesOrderHeader

# COMMAND ----------

# Aplicar a função de transformação
transformed_df = transform_Sales_SalesOrderHeader(input_df)

# Exibir o DataFrame transformado
print("Transformed DataFrame:")
display(transformed_df)

# Comparar o DataFrame transformado com o DataFrame esperado
if _compare_dataframes(transformed_df, expected_df):
    print("Teste PASSOU: O DataFrame transformado corresponde ao esperado.")
else:
    print("Teste FALHOU: O DataFrame transformado NÃO corresponde ao esperado.")


# COMMAND ----------

