# Databricks notebook source
colunas = spark.table("adventure_works_bronze.Production_Product").columns
# 
for i in colunas:
    print(f" .withColumnRenamed('{i}','{i}') ")
    # print(f", {i} AS {i}")

# COMMAND ----------

##. Ler
df = spark.table('adventure_works_bronze.Production_Product')


## Transformar
# mudar nome de coluna

df = (df .withColumnRenamed('ProductID','Product_ID') 
        .withColumnRenamed('Name','Name') 
        .withColumnRenamed('ProductNumber','Product_Number') 
        .withColumnRenamed('MakeFlag','Make_Flag') 
        .withColumnRenamed('FinishedGoodsFlag','Finished_Goods_Flag') 
        .withColumnRenamed('Color','Color') 
        .withColumnRenamed('SafetyStockLevel','Safety_Stock_Level') 
        .withColumnRenamed('ReorderPoint','Reorder_Point') 
        .withColumnRenamed('StandardCost','Standard_Cost') 
        .withColumnRenamed('ListPrice','ListPrice') 
        .withColumnRenamed('Size','Size') 
        .withColumnRenamed('SizeUnitMeasureCode','SizeUnitMeasureCode') 
        .withColumnRenamed('WeightUnitMeasureCode','WeightUnitMeasureCode') 
        .withColumnRenamed('Weight','Weight') 
        .withColumnRenamed('DaysToManufacture','DaysToManufacture') 
        .withColumnRenamed('ProductLine','ProductLine') 
        .withColumnRenamed('Class','Class') 
        .withColumnRenamed('Style','Style') 
        .withColumnRenamed('ProductSubcategoryID','ProductSubcategoryID') 
        .withColumnRenamed('ProductModelID','ProductModelID') 
        .withColumnRenamed('SellStartDate','SellStartDate') 
        .withColumnRenamed('SellEndDate','SellEndDate') 
        .withColumnRenamed('DiscontinuedDate','DiscontinuedDate') 
        .withColumnRenamed('rowguid','rowguid') 
        .withColumnRenamed('ModifiedDate','ModifiedDate') 
        .withColumnRenamed('bronze_ingestion_timestamp','bronze_ingestion_timestamp') 

)



## Salvar
df.display()


# COMMAND ----------

##. Ler
df = spark.table('adventure_works_bronze.Production_Product')


# Transformar
# mudar nome de coluna

# df = df.withColumn("NOVA_COLUNA", lit('Bernardo')) #.....


## Salvar
(df.write
    .mode('overwrite')
    .format('delta')
    .option("overwriteSchema","True")
    .saveAsTable('adventure_works_prata.Production_Product')
)


# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import lit, col

df = df.withColumn('NOVA COLUNA', lit(100)).withColumn("outra coluna", col('NOVA COLUNA'))



# COMMAND ----------

