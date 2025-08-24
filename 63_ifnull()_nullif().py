# Databricks notebook source
# MAGIC %md
# MAGIC #if null() or null if() :-
# MAGIC
# MAGIC if null is also used to handle null values just like coalesce()
# MAGIC
# MAGIC However,if null() only takes two arguments,if 1st argument is null,it returns 2nd argument.
# MAGIC
# MAGIC If both arguments are null then,It will return null
# MAGIC
# MAGIC #syntax:
# MAGIC
# MAGIC select ifnull(col_1,col_2) from df
# MAGIC
# MAGIC select ifnull(col_1,'default') from df
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select ifnull(null_values,name) from sample_table_4