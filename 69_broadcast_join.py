# Databricks notebook source
# MAGIC %md
# MAGIC #broadcast_join
# MAGIC
# MAGIC broadcast join is the optimised join technique used to join two dataframes
# MAGIC
# MAGIC This technique is ideal for joining larger dataframes with smaller data frame
# MAGIC
# MAGIC Traditional joins take longer proccessing time as they require data shuffling
# MAGIC
# MAGIC As we know,Pyspark splits the data into different nodes for parallel processing ,when you have two dataframes and you perform a join ,the data from both are distributed across multiple nodes in the cluster
# MAGIC
# MAGIC Pyspark shuffles the data as it is needed for joining since joining key may not be available in same node and to perform join, each key should be brought together on the same node 
# MAGIC
# MAGIC so with broadcast join pyspark broadcasts the smaller df to all executors and executor keeps this df in memory
# MAGIC
# MAGIC The larger dataframe is split and distributed across all the executors. so that pyspark can perform a join without shuffling any data from larger dataframe as data required for join is available on every executor
# MAGIC
# MAGIC #syntax:
# MAGIC df3=df2.join(broadcast(df1),df1.col1==df2.col2)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC