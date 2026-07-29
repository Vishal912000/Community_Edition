# Databricks notebook source
# MAGIC %md 
# MAGIC #External Tables :-
# MAGIC External tables are the tables in which databricks only manages the metadata
# MAGIC
# MAGIC In external tables data is stored in an external location (such as ADLS ,AWS S3)
# MAGIC
# MAGIC Dropping the tables only deletes the metadata 

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result")

# COMMAND ----------

#creating a external table
#Here we need to mention the path of external location that is the only difference
df.write.format("parquet").option("path",f"path_name").saveAsTable(f"database_name.table_name")
