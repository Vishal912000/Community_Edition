# Databricks notebook source
# MAGIC %md 
# MAGIC #orderBy: 
# MAGIC 1.orderby is used to sort the column of a particular table in either ascending or descending order 
# MAGIC
# MAGIC 2.It is used for sorting the data across all the partitions globally
# MAGIC
# MAGIC 3.It can be more expensive and takes more time in execution than sort() transformation
# MAGIC
# MAGIC 4.By default ,It sorts in ascending order,Also  If we don't give (ascending =True) It will sort in ascending order
# MAGIC
# MAGIC #syntax:
# MAGIC df_1=df.orderBy("col_name",ascending=True)
# MAGIC
# MAGIC or 
# MAGIC
# MAGIC df_1=df.orderBy(df.col_name.desc())
# MAGIC
# MAGIC or 
# MAGIC
# MAGIC df_1=df.orderBy(df.col_name.asc())
# MAGIC

# COMMAND ----------

https://youtu.be/cr8bcpvC8Hk?si=tNJ98jjDkgwV6alZ
Youtube link

# COMMAND ----------


df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final",header=True)
#display(df)
df_2=df.orderBy(df.country)


# COMMAND ----------

# MAGIC %md
# MAGIC #sort:
# MAGIC 1.sort is also used for sorting the data and arranging them in ascending / descending order
# MAGIC
# MAGIC 2.It is similar to orderBy transforamtion,However It doesnot do global sorting as orderBy transformation
# MAGIC
# MAGIC 3.It is used for sorting data locally across partitions
# MAGIC
# MAGIC 4.It is less expensive and takes less time in sorting compared to orderBy()
# MAGIC
# MAGIC 5.By default ,It sorts in ascending order,Also  If we don't give (ascending =True) It will sort in ascending order
# MAGIC
# MAGIC #Syntax:
# MAGIC df=df.sort(df.col,ascending=True)

# COMMAND ----------

df_sorted=df.sort(df.country)
#or 
df_sorted=df.sort(df.country)