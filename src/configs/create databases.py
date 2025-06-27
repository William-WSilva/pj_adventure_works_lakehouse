# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE adventure_works_bronze CASCADE; 
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS adventure_works_bronze
# MAGIC LOCATION '/mnt/adlsadventureworksprd/bronze/adventure_works_bronze'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE adventure_works_prata CASCADE; 
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS adventure_works_prata
# MAGIC LOCATION '/mnt/adlsadventureworksprd/prata/adventure_works_prata'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE adventure_works_ouro CASCADE; 
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS adventure_works_ouro
# MAGIC LOCATION '/mnt/adlsadventureworksprd/ouro/adventure_works_prata'

# COMMAND ----------


