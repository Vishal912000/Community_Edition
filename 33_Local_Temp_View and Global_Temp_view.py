# Databricks notebook source
# MAGIC %md #Local Temp View:-
# MAGIC It is used to create a temporary view from a dataframe 
# MAGIC
# MAGIC We need  to use createOrReplaceTempView() method to create a temp view 
# MAGIC
# MAGIC Temp view can be accessed using sql queries
# MAGIC
# MAGIC we cannot use the local temp view in the different notebooks and also once the cluster is detached we need to create a temp view again 
# MAGIC

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results_final")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Here we are accessing the temp view using the sql query and spark sql
# MAGIC select * from df_temp
# MAGIC
# MAGIC --OR  
# MAGIC --df=spark.sql(f"""select * from df_temp)

# COMMAND ----------

# MAGIC %md #Global Temp View:
# MAGIC Global temp view is also similar to local temp view ,But we will be able to access the global temp view from any notebooks unlike local temp view
# MAGIC
# MAGIC We need to use namespace same as unity catallog
# MAGIC
# MAGIC #syntax:-
# MAGIC
# MAGIC df.createOrReplaceGlobalTempView("df_name")
# MAGIC
# MAGIC #accessing global temp views
# MAGIC
# MAGIC To access global temp views we need to use global_temp database and view name  
# MAGIC

# COMMAND ----------

#Here we are creating a global temp view 
df.createOrReplaceGlobalTempView("df_temp_2")


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

#df=spark.sql(f"""select * from df_temp_2""")
#This will show table not found error

#so ,we need to use 

df_global=spark.sql(f"""select * from global_temp.df_temp_2""")