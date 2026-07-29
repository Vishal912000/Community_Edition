# Databricks notebook source
#Reading a csv file 
df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,inferSchema=True)

# Converting a dataframe to a table
df.createOrReplaceTempView("df_table")


# COMMAND ----------

# Selecting all columns using pyspark
df2=df.select('*')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
