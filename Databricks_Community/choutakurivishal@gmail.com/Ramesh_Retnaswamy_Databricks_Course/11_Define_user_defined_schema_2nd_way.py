# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",IntegerType(),True)
                                   ])

# COMMAND ----------

# MAGIC %md Previously we were using Struct Type and Struct Field ,But we can use this below string method also but using StructType and Field is prefered 

# COMMAND ----------

circuits_schema_2="circuitId INT,circuitRef STRING,name STRING,location STRING,country STRING,lat DOUBLE,lng DOUBLE,alt INT,url STRING"

# COMMAND ----------

#Reading file using schema which uses StructType and StructField
df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=circuits_schema)

# COMMAND ----------

#Reading file using schema which is a string
df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=circuits_schema_2)
