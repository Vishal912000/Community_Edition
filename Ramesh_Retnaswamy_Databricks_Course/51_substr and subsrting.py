# Databricks notebook source
# MAGIC %md
# MAGIC #substring() and substr() :
# MAGIC Substring and Substr are both used to extract the sub-part of  a column starting at a specified position.
# MAGIC
# MAGIC We can also provide length of characters in column it needs to be extracted 
# MAGIC
# MAGIC Substring indexing starts with 1 not 0
# MAGIC
# MAGIC #syntax:
# MAGIC substring(column_name,starting_position,optional(length till it needs to be extracted)
# MAGIC
# MAGIC                                          or 
# MAGIC substr(column_name,starting_position,optional(length till it needs to be extracted)

# COMMAND ----------

df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,BooleanType
from datetime import date
from pyspark.sql.functions import sum,col

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),nullable=True),
                             StructField("surname",StringType(),nullable=True)
                             ])

schema=StructType(fields=[StructField("code",StringType(),nullable=True),
                         StructField("dob",StringType(),nullable=True),
                         StructField("driverId",IntegerType(),nullable=True),
                         StructField("name",name_schema,nullable=True),
                         StructField("nationality",StringType(),nullable=True),
                         StructField("number",StringType(),nullable=True),
                         StructField("url",StringType(),nullable=True)
                         ])

# COMMAND ----------

df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_table

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --Here ,I need to extract the url separately starting link
# MAGIC select  substring(url,1,29)  as url_start from df_table
# MAGIC
# MAGIC --So, Here i am giving the starting position and the length of the characters need to be extracted

# COMMAND ----------

# MAGIC %sql
# MAGIC select substr(url,1,29) as url_start from df_table
# MAGIC
# MAGIC --So here also i am usingthe same concept but substr is used instead of substring 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  split(url,'/') as split_url from  df_table
# MAGIC --Here we are splitting the url using split url
