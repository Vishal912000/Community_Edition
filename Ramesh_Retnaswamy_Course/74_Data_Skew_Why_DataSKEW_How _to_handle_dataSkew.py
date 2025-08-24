# Databricks notebook source
https://www.youtube.com/watch?v=EQhldyLWPwI&t=603s

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Skew:-
# MAGIC Data skew is a condition in which data is unevenly distributed among partitions in the cluster
# MAGIC
# MAGIC This leads to some partitions having too much data (causing slow performance ),while others having very little data (under utilisation of resources)
# MAGIC
# MAGIC Data skew is caused by joins in most of the cases ,Joins b/w big tables require shuffling data and skew.This can lead to an extreme imbalance of work in cluster.
# MAGIC
# MAGIC It's likely that data skew is affecting a query if a query appears to be stuck finishing very few tasks

# COMMAND ----------

# MAGIC %md
# MAGIC #Why data skew is a problem?
# MAGIC #1.Performance Issue:-
# MAGIC Some tasks take much longer time to finish ,slowing down the entire job.
# MAGIC
# MAGIC #2.Uneven Resource Utilisation:-
# MAGIC Some nodes in clusters are overloaded ,while some are idle
# MAGIC
# MAGIC #3.High Shuffle costs:-
# MAGIC
# MAGIC Unbalanced partitions lead to excessive n/w communication
# MAGIC
# MAGIC #4.Memory errors:-
# MAGIC
# MAGIC large partitions may exceed memory limits

# COMMAND ----------

# MAGIC %md
# MAGIC #How to handle a data skew ?
# MAGIC 1.Salting
# MAGIC
# MAGIC 2.Re-Partitioning
# MAGIC
# MAGIC 3.Broadcast Joins
# MAGIC
# MAGIC 4.Skew join Optimisation
# MAGIC
# MAGIC 5.Increase Shuffle Partitions
# MAGIC
# MAGIC 6.Bucketing
# MAGIC
# MAGIC 7.Sampling
# MAGIC
# MAGIC 8.Using Appropriate Algorithms
# MAGIC
# MAGIC 9.Filtering Skew data separately
# MAGIC
# MAGIC 10.Adaptive Query Execution(AQE)
