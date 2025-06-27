# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES FROM adventure_works_ouro

# COMMAND ----------

df_1 = (spark
        .read
        .table('adventure_works_ouro.factinternetsales')
        .where('SalesOrderNumber = "SO43662" ')
        )

df_1.write.format('delta').mode('append').saveAsTable('adventure_works_ouro.factinternetsales')

# COMMAND ----------

df_1.orderBy('SalesOrderLineNumber', ascending=False).display()

# COMMAND ----------

df_internet_sales = spark.read.table('adventure_works_ouro.factinternetsales')


df_internet_sales.groupBy('SalesOrderNumber','SalesOrderLineNumber').count().orderBy('count', ascending=False).display()

# COMMAND ----------

df.count()

# COMMAND ----------

df = spark.read.table('adventure_works_ouro.factinternetsales')

display(df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works_ouro.factinternetsales
# MAGIC
# MAGIC where ProductKey is null

# COMMAND ----------


