# Databricks notebook source
print("carregando funções de teste")

# COMMAND ----------

from pyspark.sql import DataFrame

def _compare_dataframes(df1: DataFrame, df2: DataFrame) -> bool:
    """
    Compara dois DataFrames para verificar se são iguais linha a linha e coluna a coluna,
    e fornece detalhes sobre as diferenças, se houver.

    Parâmetros:
        df1 (DataFrame): O DataFrame resultante.
        df2 (DataFrame): O DataFrame esperado.

    Retorna:
        bool: True se os DataFrames forem iguais, False caso contrário.
    """
    try:
        # Verifica a igualdade de esquemas
        if df1.schema != df2.schema:
            print("Os esquemas dos DataFrames são diferentes.")
            print(f"Schema df1: {df1.schema}")
            print(f"Schema df2: {df2.schema}")
            return False
        
        # Verifica se há diferenças no conteúdo
        df1_except_df2 = df1.exceptAll(df2)
        df2_except_df1 = df2.exceptAll(df1)
        
        if df1_except_df2.count() == 0 and df2_except_df1.count() == 0:
            return True
        else:
            print("Diferenças encontradas entre os DataFrames:")
            if df1_except_df2.count() > 0:
                print("Linhas no DataFrame transformado que não estão no esperado:")
                df1_except_df2.display()
            
            if df2_except_df1.count() > 0:
                print("Linhas no DataFrame esperado que não estão no transformado:")
                df2_except_df1.display()
            
            return False
    
    except Exception as e:
        print(f"Erro ao comparar DataFrames: {e}")
        return False

print("Carregada a Função: _compare_dataframes(df1: DataFrame, df2: DataFrame) ")

# COMMAND ----------

