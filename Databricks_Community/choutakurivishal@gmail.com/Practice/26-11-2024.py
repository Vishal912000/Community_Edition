# Databricks notebook source
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,DateType,BooleanType,DoubleType
from pyspark.sql.functions import lit,rank,dense_rank,col,window,lag,lead

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                          StructField("circuitRef",StringType(),nullable=True),
                          StructField("name",StringType(),nullable=True),
                          StructField("location",StringType(),nullable=True),
                          StructField("lat",DoubleType(),nullable=True),
                          StructField("lng",DoubleType(),nullable=True),
                          StructField("alt",IntegerType(),nullable=True),
                          StructField("url",StringType(),nullable=True)
                          ])

# COMMAND ----------

df_2=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df3=df_2.withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

df3.write.format("parquet").mode("overwrite").save("")
