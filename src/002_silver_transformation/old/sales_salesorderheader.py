# Databricks notebook source
# MAGIC %md
# MAGIC # Habilitar a Evolução Automática de Esquema
# MAGIC
# MAGIC Neste passo, habilitamos a configuração de evolução automática de esquema para tabelas Delta no Databricks. Isso é importante para permitir que o esquema das tabelas Delta evolua automaticamente durante operações de merge, o que facilita a inclusão de novas colunas ou alterações no tipo de dados das colunas existentes sem a necessidade de recriar a tabela ou fazer alterações manuais no esquema.
# MAGIC
# MAGIC ```python
# MAGIC # Enable automatic schema evolution
# MAGIC spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
# MAGIC ```

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Enable automatic schema evolution
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino
# MAGIC
# MAGIC Neste passo, definimos as informações sobre as tabelas fonte (bronze) e destino (prata). Também configuramos as chaves primárias que serão utilizadas nas operações de merge.

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "sales_salesorderheader"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Sales_SalesOrderHeader"
target_database = "adventure_works_prata"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado
# MAGIC
# MAGIC Aqui, declaramos o schema esperado para a tabela `Sales_SalesOrderHeader`. Isso é importante para garantir que os dados transformados estejam no formato correto antes de serem inseridos na tabela de destino.

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

# MAGIC %md
# MAGIC ## Função de Transformação
# MAGIC
# MAGIC Esta função aplica as transformações necessárias na tabela `Sales_SalesOrderHeader`. 
# MAGIC
# MAGIC - Calcula o valor da coluna `TotalDue`.
# MAGIC - Define valores padrão para colunas com possíveis valores nulos.
# MAGIC - Aplica filtros para garantir a integridade dos dados.
# MAGIC - Realiza deduplicação usando uma função de janela.
# MAGIC - Faz o cast explícito dos tipos de dados das colunas.

# COMMAND ----------


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

# MAGIC %md
# MAGIC ## Aplicar Transformação
# MAGIC
# MAGIC Neste passo, aplicamos a função de transformação aos dados da tabela bronze e exibimos o resultado. Esse DataFrame transformado será utilizado para o upsert na tabela prata.

# COMMAND ----------

transformed_df = transform_Sales_SalesOrderHeader(SalesOrderHeader = bronze_source_table)

transformed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação do Schema
# MAGIC
# MAGIC A função `validate_schema` verifica se o schema do DataFrame transformado corresponde ao schema esperado. Isso é importante para garantir que os dados estejam no formato correto antes de serem inseridos na tabela de destino.

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Verificação do Schema
# MAGIC
# MAGIC Aqui aplicamos a função de validação do schema ao DataFrame transformado. Se o schema estiver correto, continuamos com o processo; caso contrário, interrompemos a execução.

# COMMAND ----------

# Verificação do Schema
is_schema_valid = validate_schema(transformed_df, expected_schema)

if is_schema_valid:
    print("O schema do DataFrame está correto.")
else:
    print("O schema do DataFrame está incorreto.")
    raise Exception("Schema validation failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação de Tabela com Chaves Primárias
# MAGIC
# MAGIC A função `create_table_with_primary_keys` cria uma tabela Delta na camada prata caso ela não exista, utilizando as chaves primárias especificadas. Isso é útil para garantir que a tabela de destino esteja pronta para receber os dados transformados.

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

# MAGIC %md
# MAGIC ## Função de Upsert na Tabela Prata
# MAGIC
# MAGIC A função `upsert_silver_table` realiza o upsert na tabela Delta da camada prata. 
# MAGIC
# MAGIC - Verifica se a tabela de destino existe. Se não, a função cria uma tabela com as chaves primárias.
# MAGIC - Constrói dinamicamente a condição de merge com base nas chaves primárias.
# MAGIC - Realiza o merge com suporte à evolução de esquema.

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
    
    print("Upsert executado com sucesso.")

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

def upsert_silver_table(transformed_df: DataFrame, target_table: str, primary_keys: list, not_matched_by_source_action: str = None, not_matched_by_source_condition: str = None) -> None:
    """
    Realiza o upsert (update e insert) na tabela Delta da camada prata,
    suportando a evolução do esquema e construindo dinamicamente a condição de merge.

    Parâmetros:
        transformed_df (DataFrame): DataFrame contendo os dados transformados para inserção na camada prata.
        target_table (str): Nome da tabela de destino.
        primary_keys (list): Lista de chaves primárias para o merge.
        not_matched_by_source_action (str, opcional): Ação a ser tomada quando uma linha da tabela de destino não tiver correspondência na tabela de origem. Pode ser "DELETE" ou "UPDATE".
        not_matched_by_source_condition (str, opcional): Condição adicional para aplicar a ação definida em not_matched_by_source_action.
    """
    
    # Verificar se a tabela de destino existe
    if not spark.catalog.tableExists(target_table):
        # Se a tabela não existe, criá-la com as chaves primárias como colunas
        transformed_df.write.format("delta").saveAsTable(target_table)
        print(f"Tabela {target_table} criada.")
        return
    
    # Construir a condição de merge com base nas chaves primárias
    merge_condition = " AND ".join([f"s.{key} = t.{key}" for key in primary_keys])

    # Carregar a tabela Delta existente
    delta_table = DeltaTable.forName(spark, target_table)

    # Iniciar a construção da operação de merge
    merge_builder = delta_table.alias("t").merge(
        transformed_df.alias("s"),
        merge_condition
    )

    # Adicionar a cláusula WHEN MATCHED
    merge_builder = merge_builder.whenMatchedUpdateAll()

    # Adicionar a cláusula WHEN NOT MATCHED (inserir novos registros)
    merge_builder = merge_builder.whenNotMatchedInsertAll()

    # Adicionar a cláusula WHEN NOT MATCHED BY SOURCE, se especificada
    if not_matched_by_source_action:
        if not_matched_by_source_action.upper() == "DELETE":
            merge_builder = merge_builder.delete()
            
    # Executar o merge
    merge_builder.execute()
    
    print("Upsert executado com sucesso.")

upsert_silver_table(transformed_df, target_table, primary_keys, not_matched_by_source_action="DELETE")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Multiple NOT MATCHED BY SOURCE clauses conditionally deleting unmatched target rows and updating two columns for all other matched rows.
# MAGIC
# MAGIC MERGE INTO target USING source
# MAGIC   ON target.key = source.key
# MAGIC   WHEN MATCHED THEN UPDATE 
# MAGIC   WHEN NOT MATCHED THEN INSERT
# MAGIC
# MAGIC   WHEN NOT MATCHED BY SOURCE AND target.marked_for_deletion THEN DELETE
# MAGIC   
# MAGIC   WHEN NOT MATCHED BY SOURCE THEN UPDATE SET target.value = DEFAULT
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

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
        transformed_df.write.format("delta").saveAsTable(target_table)
        print(f"Tabela {target_table} criada.")
        return
    
    # Registrar o DataFrame como uma view temporária
    transformed_df.createOrReplaceTempView("transformed_df_view")

    # Construir a condição de merge com base nas chaves primárias
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])

    # Obter a lista de colunas para a atualização e inserção
    update_columns = ", ".join([f"target.{col} = source.{col}" for col in transformed_df.columns])
    insert_columns = ", ".join(transformed_df.columns)
    insert_values = ", ".join([f"source.{col}" for col in transformed_df.columns])

    # Print dos valores gerados para conferência
    print(f"Condição de merge: {merge_condition}")
    print(f"Colunas para UPDATE: {update_columns}")
    print(f"Colunas para INSERT: {insert_columns}")
    print(f"Valores para INSERT: {insert_values}")

    # Construir a query SQL para o MERGE
    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING transformed_df_view AS source
        ON {merge_condition} 
        WHEN MATCHED THEN
            UPDATE SET {update_columns}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({insert_values})
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        
             
    """

    # Print da query SQL gerada
    print("Query SQL gerada para o MERGE:")
    print(merge_sql)

    # Executar a query SQL
    spark.sql(merge_sql)
    
    print("Upsert executado com sucesso.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar o Upsert na Tabela Prata
# MAGIC
# MAGIC Aqui, chamamos a função `upsert_silver_table` para realizar o upsert do DataFrame transformado na tabela de destino na camada prata.

# COMMAND ----------

# Chamar a função para realizar o upsert
upsert_silver_table(transformed_df=transformed_df, target_table=target_table, primary_keys=primary_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar os Dados na Tabela Prata
# MAGIC
# MAGIC Finalmente, fazemos uma consulta SQL para verificar os dados inseridos/atualizados na tabela `Sales_SalesOrderHeader` da camada prata.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from adventure_works_prata.Sales_SalesOrderHeader limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_prata.Sales_SalesOrderHeader

# COMMAND ----------

