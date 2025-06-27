# Databricks notebook source
print("carregando funções")

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DoubleType, DecimalType, TimestampType, ShortType)


# COMMAND ----------



def _validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
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

print("Carregada a Função: _validate_schema(df: DataFrame, expected_schema: StructType) ")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

def _upsert_silver_table(transformed_df: DataFrame, target_table: str, primary_keys: list, not_matched_by_source_action: str = None, not_matched_by_source_condition: str = None) -> None:
    """
    Realiza o upsert (update e insert) na tabela Delta da camada prata,
    suportando a evolução do esquema e construindo dinamicamente a condição de merge.

    Parâmetros:
        transformed_df (DataFrame): DataFrame contendo os dados transformados para inserção na camada prata.
        target_table (str): Nome da tabela de destino.
        primary_keys (list): Lista de chaves primárias para o merge.
        not_matched_by_source_action (str, opcional): Ação a ser tomada quando uma linha da tabela de destino não tiver correspondência na tabela de origem. Pode ser "DELETE" ou "UPDATE".
        not_matched_by_source_condition (str, opcional): Condição adicional para aplicar a ação definida em not_matched_by_source_action. -- use t.column = s.column -- t -> target / s -> source
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

    # Se o parâmetro not_matched_by_source_action for "DELETE", adicionar a lógica para deletar linhas
    if not_matched_by_source_action and not_matched_by_source_action.upper() == "DELETE":
        # Obter as chaves das linhas na tabela de destino que não têm correspondência na tabela de origem
        unmatched_rows = delta_table.toDF().alias("t").join(
            transformed_df.alias("s"),
            on=[F.col(f"t.{key}") == F.col(f"s.{key}") for key in primary_keys],
            how="left_anti"
        )

        # Aplicar a condição adicional de exclusão, se fornecida
        if not_matched_by_source_condition:
            unmatched_rows = unmatched_rows.filter(not_matched_by_source_condition)

        # Executar a exclusão das linhas não correspondentes
        delta_table.alias("t").merge(
            unmatched_rows.alias("s"),
            merge_condition
        ).whenMatchedDelete().execute()

    # Executar o merge
    merge_builder.execute()
    
    print("Upsert executado com sucesso.")


print("Carregada a Função: _upsert_silver_table(transformed_df: DataFrame, target_table: str, primary_keys: list, not_matched_by_source_action: str = None, not_matched_by_source_condition: str = None) ")