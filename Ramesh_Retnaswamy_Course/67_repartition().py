# Databricks notebook source
# MAGIC %md
# MAGIC #repartition():-
# MAGIC repartition function is used to re-distribute the data across a specified number of partitions in a dataframe
# MAGIC
# MAGIC repartition is used to increase or decrease number of partitions in spark
# MAGIC
# MAGIC repartition always shuffles the data and builds a new partitions from scratch 
# MAGIC
# MAGIC repartition results in almost equal size partitions
# MAGIC
# MAGIC Due to full shuffle,It is not good for performance in some cases 
# MAGIC But as it creates equal sized partitions ,It might be good for performance in some cases.
# MAGIC
# MAGIC #syntax:-
# MAGIC df_1=df.repartition(num_of_partiitions)
# MAGIC
# MAGIC #Ex:-
# MAGIC df_1=df.repartition(4)
# MAGIC
# MAGIC By using repartition wisely ,you can optimise data distribution,enhance parallelism etc
# MAGIC
# MAGIC
# MAGIC
