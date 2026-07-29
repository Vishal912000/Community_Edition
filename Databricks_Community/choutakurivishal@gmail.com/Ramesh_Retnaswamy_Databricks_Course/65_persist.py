# Databricks notebook source
# MAGIC %md
# MAGIC #persist():
# MAGIC persist is another programming mechansism through which it is possible to store the data either in memory or disk or combination of both 
# MAGIC
# MAGIC It is also similar to cache but more flexible and customisable than cache
# MAGIC
# MAGIC It allows us to specify storage levels (Memory and disk,Memory only etc based on needs )
# MAGIC
# MAGIC #Different storage levels:
# MAGIC 1.Memory_only
# MAGIC 2.Memory_only_ser
# MAGIC 3.Memory_and_disk
# MAGIC 4.Memory_and_disk_ser
# MAGIC 5.Disk_only
# MAGIC
# MAGIC
# MAGIC #syntax:
# MAGIC df.persist()
# MAGIC
# MAGIC df.persist(storage_level.Memory_and_disk_ser)
