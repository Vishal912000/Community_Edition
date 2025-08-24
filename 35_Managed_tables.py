# Databricks notebook source
# MAGIC %md
# MAGIC #Managed tables:-
# MAGIC
# MAGIC Managed tables are tables where both metadata and the data are managed by databricks
# MAGIC
# MAGIC Data is stored in databricks managed file system
# MAGIC
# MAGIC Dropping the table deletes both data and metadata from database
# MAGIC
# MAGIC

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo
# MAGIC --Here we are creating a database if it doesn't exist 

# COMMAND ----------

# creation of managed tables using python 
#Here we are saving the dataframe to a table 
#It is same as saving data into a file instead  of save we are using saveAsTable  
df.write.format("parquet").mode("overwrite").saveAsTable(f"demo.races_result_python")

# COMMAND ----------

df=spark.sql(f"select * from demo.races_result_python")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.races_result_python
# MAGIC
# MAGIC --Here we can see in the Type whether a table is a managed table or an external table  as this describes all details of a table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating a managed table using SQL
# MAGIC create table demo.races_results_sql
# MAGIC as
# MAGIC select * from demo.races_result
# MAGIC where race_year=2020
# MAGIC
# MAGIC --Here we are creating a table using the already available  table in the database,After creation of table we need to insert data into table if we create using sql 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.races_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table demo.races_results_sql