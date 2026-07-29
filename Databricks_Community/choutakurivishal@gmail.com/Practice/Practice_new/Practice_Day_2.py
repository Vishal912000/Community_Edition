# Databricks notebook source
### **New Combination 2**
1. Import all the required modules  
2. Create a schema (integer, string, double, date)  
3. Create a data list  
4. Create a data frame using data list and schema  
5. Run any notebook  
6. Create a delta table using any dataframe  
7. Read a CSV file  
8. Read a JSON file  
9. Perform aggregate functions  
10. Perform rank functions  
11. Perform nested JSON file ingestion 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType,DoubleType
from datetime import date
from pyspark.sql.functions import col

# COMMAND ----------

dataframe_schema=StructType(fields=[StructField("Id",IntegerType(),nullable=True),
                                    StructField("name",StringType(),nullable=True),
                                    StructField("Height",DoubleType(),nullable=True),
                                    StructField("D_o_b",DateType(),nullable=True)
                                    ])

# COMMAND ----------

sample_data=[(1,"Vishal",5.11,date(2000,1,9)),
              (2,"Akhilesh",5.9,date(2001,8,31)),
              (3,"Rahul",5.11,date(1998,11,2)),
              (4,"Varun",6.0,date(2000,4,16))
              ]

# COMMAND ----------

sample_df=spark.createDataFrame(sample_data,dataframe_schema)
display(sample_df)

# COMMAND ----------

# MAGIC %run  "/Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/49_concat and concat_ws"

# COMMAND ----------

sample_df.write.format("delta").mode("overwrite").saveAsTable(f"sample_table")

# COMMAND ----------

csv_df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

csv_schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                            StructField("circuitRef",StringType(),nullable=True),
                            StructField("name",StringType(),nullable=True),
                            StructField("location",StringType(),nullable=True),
                            StructField("country",StringType(),nullable=True),
                            StructField("lat",StringType(),nullable=True),
                            ])

# COMMAND ----------

csv_df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=csv_schema)
display(csv_df)

# COMMAND ----------

json=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,inferSchema=True)

# COMMAND ----------

nest_schema=StructType(fields=[StructField("forename",StringType(),nullable=True),
                               StructField("surname",StringType(),nullable=True)
                               ])


json_schema=StructType(fields=[StructField("code",StringType(),nullable=True),
                               StructField("dob",StringType(),nullable=True),
                               StructField("driverId",StringType(),nullable=True),
                               StructField("name",nest_schema,nullable=True
                                           )])

# COMMAND ----------

json=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=json_schema)
display(json)

# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

sum_sample=sample_df.agg(sum("Height"))
display(sum_sample)
