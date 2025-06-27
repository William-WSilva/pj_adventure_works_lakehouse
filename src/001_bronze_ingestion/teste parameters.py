# Databricks notebook source
dbutils.widgets.text('name','','')
name = dbutils.widgets.get("name")

dbutils.widgets.text('idade','','')
idade = dbutils.widgets.get("idade")


# COMMAND ----------



# COMMAND ----------

print(f"{name} tem {idade} anos")