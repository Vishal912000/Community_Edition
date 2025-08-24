# Databricks notebook source
# MAGIC %md 
# MAGIC withColumnRenamed ():-
# MAGIC It is used to rename the existing column name of with a given new column
# MAGIC
# MAGIC
# MAGIC When we apply the withColumnRenamed method, the specified column names are renamed in the new DataFrame, while the columns that are not renamed remain the same and are also included in the new DataFrame
# MAGIC
# MAGIC Syntax :-df_renamed=df.withColumnRenamed("existing_col_name","new_col_name")

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_rename=df.withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

#Renaming multiple columns at once


df_renamed=df.withColumnRenamed("circuitId","circuit_id")\
                .withColumnRenamed("circuitRef","circuit_ref")\
                .withColumnRenamed("lat","latitude")\
                .withColumnRenamed("lng","longitude")\
                .withColumnRenamed("alt","altitude")