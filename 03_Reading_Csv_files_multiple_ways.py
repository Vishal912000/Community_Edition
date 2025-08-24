# Databricks notebook source
df1=spark.read.csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
display(df1)

# COMMAND ----------

df_2=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
display(df_2)

# COMMAND ----------

df_3=spark.read.csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)
display(df_3)

# COMMAND ----------

df_4=spark.read.format("csv").option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
display(df_4)

# COMMAND ----------

df_5=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)
display(df_5)

# COMMAND ----------

df_6=spark.read.option("delimeter" ,",").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)
display(df_6)