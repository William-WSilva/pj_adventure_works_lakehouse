# Databricks notebook source


# COMMAND ----------

# ADLS
storage_acount_name = 'wsadlsadwprd'

# Lista conteúdo se o mount funcionar
try:
    display(dbutils.fs.ls(f"/mnt/{storage_acount_name}/adw/"))
except Exception as e:
    print("Erro ao listar o conteúdo:", e)

# COMMAND ----------

%sql
-- ATENÇÃO!!! DELETAR BANCO DE DADOS ???
USE CATALOG hive_metastore;
DROP DATABASE IF EXISTS adventure_works_silver CASCADE

# COMMAND ----------

%sql
USE CATALOG hive_metastore;

CREATE DATABASE IF NOT EXISTS adventure_works_bronze
LOCATION '/mnt/wsadlsadwprd/adw/001_bronze/adventure_works_bronze'

# COMMAND ----------

%sql
USE CATALOG hive_metastore;

CREATE DATABASE IF NOT EXISTS adventure_works_prata
LOCATION '/mnt/wsadlsadwprd/adw/002_silver/adventure_works_prata'

# COMMAND ----------

%sql
USE CATALOG hive_metastore;

CREATE DATABASE IF NOT EXISTS adventure_works_ouro
LOCATION '/mnt/wsadlsadwprd/adw/003_gold/adventure_works_ouro'