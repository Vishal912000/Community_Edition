# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #cte or Common table expressions:-
# MAGIC A cte or common table expression is a named temporary result ser that can be refernced within the scope of a single query
# MAGIC (select,inster,update or delete statement)
# MAGIC
# MAGIC It is defined by using the with clause before the main query
# MAGIC
# MAGIC A CTE can be used to simplify the query logic,improve readability and avoid repeated computations 
# MAGIC
# MAGIC CTE are used for code re-usability
# MAGIC
# MAGIC #syntax:-
# MAGIC
# MAGIC with cte_name as (
# MAGIC   select * from employee
# MAGIC )
# MAGIC
# MAGIC select * from cte_name;
# MAGIC
# MAGIC