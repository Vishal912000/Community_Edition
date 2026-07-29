# Databricks notebook source
# MAGIC %md
# MAGIC #concat():
# MAGIC concat combines multiple strings ,or multiple columns into a single string in a column.
# MAGIC
# MAGIC To add an null value , we can be add as an empty string in enclosed in  quotes (" ")
# MAGIC
# MAGIC #syntax:
# MAGIC
# MAGIC select concat(col1,col2) from database_name.table_name
# MAGIC
# MAGIC select concat(col1," ",col2) from database_name.table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.demo order by id
# MAGIC
# MAGIC --Table used in practice day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat(name," ",Height),* from default.demo

# COMMAND ----------

# MAGIC %md
# MAGIC # concat_ws():
# MAGIC
# MAGIC Concat_ws is also used to combine two strings /column into a single string / column
# MAGIC
# MAGIC concat_ws is concat with separator 
# MAGIC
# MAGIC But,In concat_ws we will use a separator which needs to mentioned at beginning in syntax
# MAGIC
# MAGIC separator can be a comma,slash or anything
# MAGIC
# MAGIC #syntax:
# MAGIC select concat_ws('separator',col1,col2) from table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat_ws(",",Name,Height) as Name_and_Height from default.sample_table
