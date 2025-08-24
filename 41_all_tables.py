# Databricks notebook source
circuits_table=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final",header=True)
constructor_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructor_final",header=True)
driver_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers_final",header=True)
lap_times_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/lap_times_final",header=True)
pit_stops_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/pit_stops_final",header=True)
qualifying_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/qualifying_final",header=True)
races_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_final",header=True)
races_result=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result",header=True)
result_final=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results_final",header=True)


# COMMAND ----------

# MAGIC %sql
# MAGIC create database if  not exists demo

# COMMAND ----------

#Here we are removing the previously created tables as we are using the community version ,the tables are  getting stored as files  and we are not able to use them again as tables

#so we are removing those files and creating the database again and tables again 
#bcse this is community version we are facing this issue else we won't face this
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/circuits", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/constructors", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/drivers", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/laptimes", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/pit_stops", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/qualifying", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/races", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/races_result", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/demo.db/result", True)


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo

# COMMAND ----------

# %sql
# --use this if we require to drop tables 
# DROP table demo.constructors 

# COMMAND ----------

circuits_table.write.format("delta").mode("overwrite").saveAsTable(f"demo.circuits")
constructor_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.constructors")
driver_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.drivers")
lap_times_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.laptimes")
pit_stops_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.pit_stops")
qualifying_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.qualifying")
races_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.races")
races_result.write.format("delta").mode("overwrite").saveAsTable(f"demo.races_result")
result_final.write.format("delta").mode("overwrite").saveAsTable(f"demo.result")

# COMMAND ----------

#this will save the tables in parquet format
#circuits_table.write.format("parquet").mode("overwrite").saveAsTable(f"demo.circuits_parquet")

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc database demo

# COMMAND ----------

# %sql
# --
# desc extended demo.circuits

# COMMAND ----------

# %sql
# desc extended demo.circuits_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuits