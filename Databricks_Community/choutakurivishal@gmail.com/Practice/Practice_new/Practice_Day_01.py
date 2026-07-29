# Databricks notebook source
1. Import all the required modules  
2. Create a schema (integer, string, double, date)  
3. Create a data list  
4. Create a data frame using data list and schema  
5. Run any notebook  
6. Create a delta table using any dataframe  
7. Read a CSV file  
8. Read a JSON file  
9. Perform select, with column, with column renamed, drop method  
10. Perform joins  
11. Create widgets 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType
from datetime import date 
from pyspark.sql.functions import col

# COMMAND ----------

dataframe_schema=StructType(fields=[StructField("id",IntegerType(),nullable=True),
                                    StructField("name",StringType(),nullable=True),
                                    StructField("Height",DoubleType(),nullable=True),
                                    StructField("D.o.b",DateType(),nullable=True)
                                    ])

# COMMAND ----------

school_data=[(1,"Vishal",5.11,date(2000,1,9)),
             (2,"Akhilesh",5.8,date(2001,8,31)),
             (3,"Rahul",5.8,date(1998,11,19)),
             (4,"Varun",6.0,date(2000,4,16)
              )]

# COMMAND ----------

df=spark.createDataFrame(school_data,dataframe_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(f"default.demo")

# COMMAND ----------

df2=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",inferSchema=True,header=True)

# COMMAND ----------

schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                          StructField("circuitRef",StringType(),nullable=True),
                          StructField("name",StringType(),nullable=True),
                          StructField("location",StringType(),nullable=True)
                          ])

# COMMAND ----------

df2=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=schema)

# COMMAND ----------

json_schema=StructType(fields=[StructField("constructorId",IntegerType(),nullable=True),
                               StructField("constructorRef",StringType(),nullable=True),
                               StructField("name",StringType(),nullable=True),
                               StructField("nationality",StringType(),nullable=True),
                               StructField("url",StringType(),nullable=True)
                               ])

# COMMAND ----------

json_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructors.json",header=True,schema=json_schema)

# COMMAND ----------

df_nested=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,inferSchema=True)

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),nullable=True),
                               StructField("surname",StringType(),nullable=True)
                               ])


nested_json=StructType(fields=[StructField("code",StringType(),nullable=True),
                              StructField("dob",DateType(),nullable=True),
                              StructField("driverId",IntegerType(),nullable=True),
                              StructField("driverRef",StringType(),nullable=True),
                              StructField("name",name_schema,nullable=True),
                              StructField("nationality",StringType(),nullable=True),
                              StructField("number",IntegerType(),nullable=True),
                              StructField("url",StringType(),nullable=True)
                              ])

# COMMAND ----------

nested_json_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=nested_json)

display(nested_json_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

json_df_with=json_df.withColumn("ingestion_time",current_timestamp())
display(json_df_with)

# COMMAND ----------

json_df_renamed=json_df_with.withColumnRenamed("constructorId","constructor_id" )\
                             .withColumnRenamed("constructorRef","constructor_ref")

# COMMAND ----------

json_df_drop=json_df_renamed.drop("url")

# COMMAND ----------

json_df_filter=json_df_drop.filter("nationality='Indian'")

# COMMAND ----------

json_df_join=json_df_filter.join(nested_json_df,json_df_filter.nationality==nested_json_df.nationality,"left")
display(json_df_join)

# COMMAND ----------

json_inner=json_df_filter.join(nested_json_df,json_df_filter.nationality==json_df_filter.nationality,"right")

# COMMAND ----------

dbutils.widgets.text(name='fruits',defaultValue="",label=None)

# COMMAND ----------

fruits=dbutils.widgets.get('fruits')
display(fruits)
