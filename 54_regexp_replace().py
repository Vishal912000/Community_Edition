# Databricks notebook source
# MAGIC %md
# MAGIC #regexp_replace():-
# MAGIC regexp replace is used to search and replace the parts of a  column which are of the same pattern with a new pattern
# MAGIC
# MAGIC pattern can include strings,numbers ,special characters etc
# MAGIC
# MAGIC #syntax:
# MAGIC regexp_replace(col_name,pattern_in_column,replacement_pattern)
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,BooleanType
from datetime import date
from pyspark.sql.functions import sum,col

# COMMAND ----------

df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,inferSchema=True)

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),nullable=True),
                               StructField("surame",StringType(),nullable=True)
                               ])

json_schema=StructType(fields=[StructField("code",StringType(),nullable=True),
                          StructField("dob",StringType(),nullable=True),
                          StructField("driverId",IntegerType(),nullable=True),
                          StructField("driverRef",StringType(),nullable=True),
                          StructField("name",name_schema,nullable=True),
                          StructField("nationality",StringType(),nullable=True),
                          StructField("number",StringType(),nullable=True),
                          StructField("url",StringType(),nullable=True)
                          ])

# COMMAND ----------

df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=json_schema)

# COMMAND ----------

sample_schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                                 StructField("age",IntegerType(),nullable=True),
                                 StructField("sex",StringType(),nullable=True),
                                 StructField("Height",StringType(),nullable=True),
                                 StructField("dob",DateType(),nullable=True),
                                 StructField("patter",StringType(),nullable=True)
                                 ])

# COMMAND ----------

sample_data=[("Choutakuri Vishal",24,"Male",5.11,date(2000,1,9),"Hello1234@#$Vishal"),
      ("Choutakuri Akhilesh",23,"Male",5.9,date(2001,8,31),"Hello2222@#Akhilesh"),
      ("Choutakuri Shreesha",18,"Female",5.10,date(2005,5,11),"null")
      ]

# COMMAND ----------

sample_df=spark.createDataFrame(sample_data,sample_schema)
display(sample_df)

# COMMAND ----------

sample_df.createOrReplaceTempView("sample_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select regexp_replace(name,'a'," ") from sample_table
# MAGIC
# MAGIC --So here i am searching for a and replacing it with space 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  regexp_replace(patter,'[a-zA-Z]'," ") from sample_table
# MAGIC
# MAGIC --here we are capturing the searching for all small a-z and capital A-Z characters and replacing them with space
# MAGIC
# MAGIC --We can also give a not which will do the reverse 

# COMMAND ----------

# MAGIC %sql
# MAGIC select regexp_replace(patter,'[^a-zA-Z0-9]'," ") from sample_table
# MAGIC
# MAGIC --So,Here we are searching for the regexpression other than the a-z A-Z 0-9 and replacing them with spaces