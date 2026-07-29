# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #Filter:-
# MAGIC 1)Filter keyword is used to filter the rows using condition given inside the parenthesis 
# MAGIC
# MAGIC 2)It is similar to WHERE clause in SQL  ,Infact we can also use where instead of filter in databricks
# MAGIC
# MAGIC #syntax:
# MAGIC
# MAGIC df=df.filter("condition")

# COMMAND ----------

df=spark.read.format('parquet').load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_fi")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#We can write filter condition using multiple ways same as select condition 
# 1st way
df_filtered=df.filter("year=2019")
display(df_filtered)


#2nd way
#This below  code is written in python so we need to use python format
df_filtered_1=df.filter(df.year==2019)
display(df_filtered_1)

#3rd way
df_filtered_2=df.filter(col('year')==2019)
display(df_filtered_2)




# COMMAND ----------

#Instead of filter keword we can also use where ,It is an alias name

df_filtered_w=df.where("year=2019")
#display(df_filtered_w)

df_filtered_1_w=df.where(df.year==2019)
#display(df_filtered_1_w)

df_filtered_2_w=df.where(col("year")==2019)
#display(df_filtered_2_w)

# COMMAND ----------

#Filtering Multiple column data

df_filtered_4=df.filter("year =2019 and round=3")
display(df_filtered_4)

df_filtered_5=df.filter((col("year") ==2019) & (col("round") ==3))
display(df_filtered_5)
