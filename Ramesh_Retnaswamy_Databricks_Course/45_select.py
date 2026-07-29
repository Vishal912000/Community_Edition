# Databricks notebook source
# MAGIC %md # Select 
# MAGIC select command is used to select the data from the table 
# MAGIC
# MAGIC  ( * asterisk or star ) is used to select  all columns from the table
# MAGIC
# MAGIC  If we need to select a particular column we need to give the column name 
# MAGIC
# MAGIC
# MAGIC # Syntax:
# MAGIC select * from database_name.table_name
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_id,driver_name from demo.drivers
