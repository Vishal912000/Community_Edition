# Databricks notebook source
i=input("please enter name") 
print(i)

# COMMAND ----------

df=spark.read.format('csv').load('file_path',header=True,inferSchema=True)

# COMMAND ----------

df.write.format('csv').mode('overwrite')
