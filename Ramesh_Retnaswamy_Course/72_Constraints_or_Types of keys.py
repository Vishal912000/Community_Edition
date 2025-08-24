# Databricks notebook source
# MAGIC %md
# MAGIC Constraints :-Constraints in SQL are rules applied to columns or tables to enforce data integrity and ensure the validity of data 
# MAGIC
# MAGIC There are 7 types of constraints
# MAGIC
# MAGIC
# MAGIC 1)Not null
# MAGIC
# MAGIC 2)Unique
# MAGIC
# MAGIC 3)Primary Key
# MAGIC
# MAGIC 4)Foreign Key
# MAGIC
# MAGIC 5)Check
# MAGIC
# MAGIC 6)default
# MAGIC
# MAGIC 7)Index
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #1)Not null:-
# MAGIC Ensures that a column cannot contain null values
# MAGIC
# MAGIC #2)Unique :-
# MAGIC Ensures that all values in a column must be unique
# MAGIC
# MAGIC #3)Primary Key:-
# MAGIC Combination of a not null + unique key
# MAGIC
# MAGIC #4)Check:-
# MAGIC Verifies that a column's value meets a specific condition
# MAGIC
# MAGIC #5)Default :-
# MAGIC Sets a default value for a column if no value is provided default value is used during insertion
# MAGIC
# MAGIC #6)Foreign Key:-
# MAGIC Ensures the referntial integrity,Establishes a realtion b/w columns in two tables
# MAGIC
# MAGIC #7)Index:-
# MAGIC Improves the performance by vreating an index on a column or a set of columns
