# Databricks notebook source
### **New Combination 3**
1. Import all the required modules  
2. Create a schema (integer, string, double, date)  
3. Create a data list  
4. Create a data frame using data list and schema  
5. Run any notebook  
6. Create a delta table using any dataframe  
7. Read a CSV file  
8. Read a JSON file  
9. Perform joins  
10. Create a temporary view (global temp view)  
11. Perform SQL (insert, select, merge, update, delete)  

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,BooleanType,DateType
from datetime import date
from pyspark.sql.functions import sum,column

# COMMAND ----------

sample_schema=StructType(fields=[StructField("Id",IntegerType(),nullable=True),
                                 StructField("name",StringType(),nullable=True),
                                 StructField("Height",DoubleType(),nullable=True),
                                 StructField("Age",IntegerType(),nullable=True),
                                 StructField("D_o_b",DateType(),nullable=True)
                                 ])

# COMMAND ----------

sample_data=[(1,"vishal choutakuri",5.10,24,date(2000,1,9)),
             (2,"akhilesh choutakuri",5.8,23,date(2001,8,31)),
             (3,"rahul bodigam",5.9,25,date(1998,11,2)),
             (4,"varun galigama",6.0,24,date(2000,4,16))
             ]

# COMMAND ----------

df=spark.createDataFrame(sample_data,sample_schema)
display(df)

# COMMAND ----------

# MAGIC %run /Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/50_split

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(f"sample_dataa")

# COMMAND ----------

csv_fi=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

csv_schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                              StructField("circuitRef",StringType(),nullable=True),
                              StructField("name",StringType(),nullable=True),
                              StructField("location",StringType(),nullable=True),
                              StructField("country",StringType(),nullable=True),
                              StructField("lat",DoubleType(),nullable=True),
                              StructField("long",DoubleType(),nullable=True),
                              StructField("alt",IntegerType(),nullable=True)
                              ])

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=csv_schema)

# COMMAND ----------

df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,inferSchema=True)

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),nullable=True),
                               StructField("surname",StringType(),nullable=True)
                               ])

json_schema=StructType(fields=[StructField("code",StringType(),nullable=True),
                               StructField("dob",StringType(),nullable=True),
                               StructField("driverId",StringType(),nullable=True),
                               StructField("driverRef",StringType(),nullable=True),
                               StructField("name",name_schema,nullable=True),
                               StructField("nationality",StringType(),nullable=True),
                               StructField("number",StringType(),nullable=True),
                               StructField("url",StringType(),nullable=True)])

# COMMAND ----------

json_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=json_schema)

# COMMAND ----------

json_df=json_df.filter("driverId > 30")

# COMMAND ----------

json_df_csv=json_df.join(df,json_df.driverId==df.driverId,"right")
display(json_df_csv)
