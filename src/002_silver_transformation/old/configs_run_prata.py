# Databricks notebook source
# Criação do dicionário com todas as tabelas e os atributos 'ativo' e 'notebook_path'
database_tables = {
    "Sales_SalesOrderHeader": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_SalesOrderHeader"
    },
    "Sales_SalesOrderDetail": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_SalesOrderDetail"
    },
    "Production_Product": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Production_Product"
    },
    "Production_ProductSubcategory": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Production_ProductSubcategory"
    },
    "Production_ProductModel": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Production_ProductModel"
    },
    "Production_ProductDescription": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Production_ProductDescription"
    },
    "Sales_Customer": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_Customer"
    },
    "Sales_SpecialOffer": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_SpecialOffer"
    },
    "Sales_Currency": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_Currency"
    },
    "Sales_SalesTerritory": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_SalesTerritory"
    },
    "Sales_SalesReason": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Sales_SalesReason"
    },
    "Person_Address": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Person_Address"
    },
    "Person_Person": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Person_Person"
    },
    "Person_EmailAddress": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Person_EmailAddress"
    },
    "Person_PersonPhone": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Person_PersonPhone"
    },
    "Person_StateProvince": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Person_StateProvince"
    },
    "Person_CountryRegion": {
        "ativo": True,
        "notebook_path": "caminho/para/notebook_Person_CountryRegion"
    }
}

# COMMAND ----------

for table in database_tables:
    path = database_tables[table]['notebook_path']
    if database_tables[table]['ativo']:
        print(f'Executando tabela: {table}')

# COMMAND ----------

