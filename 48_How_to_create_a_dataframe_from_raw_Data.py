# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a dataframe 
# MAGIC Step 1:
# MAGIC Create a schema 
# MAGIC
# MAGIC Step 2 : create a list which contains data for each row based on schema
# MAGIC
# MAGIC Step 3 : create a dataframe 
# MAGIC Syntax: 
# MAGIC
# MAGIC df_name=spark.createDataFrame(data_list.schema_name)

# COMMAND ----------

# To create a dataframe we need to first create a schema and then add data into columns 

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DateType,DoubleType
from datetime import date


# COMMAND ----------

#Here we are creating the schema for a data frame
schema=StructType(fields=[StructField("Id",IntegerType(),nullable=True),
                          StructField("Name",StringType(),nullable=True),
                          StructField("Sex",StringType(),nullable=True),
                          StructField("D.O.B",DateType(),nullable=True),
                          StructField("Weight",DoubleType(),nullable=True)
                  ])

# COMMAND ----------

#Here i am adding data using a list
data=[(1,"Vishal","Male",date(2000,1,9),67.00),
      (2,"Akhilesh","Male",date(2001,8,31),62.00),
      (3,"Varun","Male",date(2000,4,16),64.50),
      (4,"Rahul","Male",date(1998,11,2),67.55)
      ]

#Leading zeroes are not permitted in date format in databricks,It will give an error 

#01 is not permitted we need to give 1 only 

#and in date format we need to give commas only for separating year,month and day date(2000,1,9) and default is year,month,day 

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

display(df)