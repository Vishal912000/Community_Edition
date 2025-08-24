# Databricks notebook source
# MAGIC %md
# MAGIC #coalesce():
# MAGIC coalesce function in databricks reduces the no. of partitions 
# MAGIC
# MAGIC coalesce doesn't require a full shuffle,coalesce combines few partitions or shuffles data from few partitions thus avoiding full shuffle
# MAGIC
# MAGIC Due to partition merge it produces uneven size of partitions
# MAGIC
# MAGIC Since full shuffle is avioded ,coalesce is more performance efficient than repartition 
# MAGIC
# MAGIC #Syntax:-
# MAGIC
# MAGIC df_1=df.coalesce(num_of_partitions)
# MAGIC
# MAGIC df_1=df.coalesce(2)
# MAGIC
# MAGIC
