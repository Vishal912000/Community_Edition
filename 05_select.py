# Databricks notebook source
# MAGIC %md 
# MAGIC Select Statement is used to selct the particular columns of a dataframe
# MAGIC
# MAGIC In pyspark we can use select statement using multiple ways 
# MAGIC
# MAGIC
# MAGIC Things to do:
# MAGIC
# MAGIC   1.Define the user defined schema 
# MAGIC
# MAGIC   2.Read a csv file and save it to a dataframe
# MAGIC
# MAGIC   3.Select some columns from the dataframe
# MAGIC     
# MAGIC   4.Different ways to select
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

user_schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=False),
                               StructField("circuitRef",StringType(),nullable=True),
                               StructField("name",StringType(),nullable=True),
                               StructField("location",StringType(),nullable=True),
                               StructField("country",StringType(),nullable=True),
                               StructField("lat",DoubleType(),nullable=True),
                               StructField("lng",DoubleType(),True),
                               StructField("alt",IntegerType(),True),
                               StructField("url",StringType(),True)
                               ])

# COMMAND ----------

# Reading a csv file and storing it in a dataframe
df_1=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=user_schema)


# COMMAND ----------

#selecting all columns

#We can use * in quotes to select all the columns
df_2=df_1.select("*")

# COMMAND ----------

#1st method
#Here I am Selecting 1st 8 columns 
df_3=df_1.select("circuitId",
                 "circuitRef",
                 "name",
                 "location",
                 "country",
                 "lat",
                 "lng",
                 "alt"
                 )
#display(df_3)
#In this method we cannot modify the column name

# COMMAND ----------

#2nd method
#Here we are selecting columns by giving the df name 
df_4=df_1.select(df_1.circuitId,
                 df_1.circuitRef,
                 df_1.name,
                 df_1.location
                 )

#We can also modify the column name using this method

df_5=df_1.select(df_1.circuitId.alias("circuit_id"),
                 df_1.circuitRef.alias("circuit_ref")
                 )

# COMMAND ----------

#3rd method
#It is similar to 2nd method but here we are using square brackets instead of .(dot)
df_6=df_1.select(df_1["circuitId"],
                 df_1["circuitRef"],
                 df_1["name"]
                 )

#We also modify column_name same as 2nd method
df_7=df_1.select( df_1["circuitId"].alias("circuit_id"),
                  df_1["circuitRef"].alias("circuit_ref")
                 )

# COMMAND ----------

#4th method 
#This is a bit easy method and similar to others 
#here we need to import col function
from pyspark.sql.functions import col

df_8=df_1.select(col("circuitId"),
                 col("circuitRef"),
                 col("name"),
                 col("location"),
                 col("country")
                 )
#here instead of giving dataframe name we can use col keyword
#also we can modify the col name as previous methods

df_9=df_1.select(   col("circuitId").alias("circuit_id"),
                    col("circuitRef").alias("circuit_ref")
                 )
###### WE ARE USING 4th method in next lessons

# COMMAND ----------

# 5th method using spark sql
#to use this method we need to convert df_1 to a temporary view

df_1.createOrReplaceTempView("df_1_view")
df_10=spark.sql(f"""select circuitId,
                            circuitRef,
                            name,
                            location,
                            country
                            from
                            df_1_view
                            """
                            )

# we can also change the col name same as before
df_11=spark.sql(f""" select 
                            circuitId as circuit_id,
                            circuitRef as circuit_ref
                            from
                            df_1_view
                            """
                            )