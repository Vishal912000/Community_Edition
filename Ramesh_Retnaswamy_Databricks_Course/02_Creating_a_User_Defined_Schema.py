# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

d=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True)

# COMMAND ----------

display(d)

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId", IntegerType(),nullable=False),
                                    StructField("circuitRef",StringType(),nullable=True),
                                     StructField("name",StringType(),nullable=True),
                                     StructField("location",StringType(),nullable=True),
                                     StructField("country",StringType(),nullable=True),
                                     StructField("lat",DoubleType(),nullable=True),
                                     StructField("lng",DoubleType(),nullable=True),
                                     StructField("alt",StringType(),nullable=True),
                                     StructField("url",StringType(),nullable=True)
                                     ])

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=circuits_schema)


#or 

#df=spark.read.format("csv").option("header",True).schema(circuits_schema).load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/#circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

circuits_2=StructType(fields=[StructField("circuitId",IntegerType(),False),
                              StructField("circuitRef",StringType(),True),
                              StructField("name",StringType(),True),
                              StructField("location",StringType(),True),
                              StructField("lat",DoubleType(),True),
                              StructField("lng",DoubleType(),True),
                              StructField("alt",StringType(),True),
                              StructField("url",StringType(),True)
                              ])

# COMMAND ----------


