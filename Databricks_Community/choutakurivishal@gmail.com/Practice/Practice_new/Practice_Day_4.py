# Databricks notebook source
### **New Combination 4**
1. Import all the required modules  
2. Create a schema (integer, string, double, date)  
3. Create a data list  
4. Create a data frame using data list and schema  
5. Run any notebook  
6. Create a delta table using any dataframe  
7. Read a CSV file  
8. Read a JSON file  
9. Perform group by and partition by  
10. Perform nested JSON file ingestion  
11. Perform aggregate functions  

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import sum,col
from datetime import date

# COMMAND ----------

sample_schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                                 StructField("number",IntegerType(),nullable=True),
                                 StructField("age",IntegerType(),nullable=True),
                                 StructField("sex",StringType(),nullable=True),
                                 StructField("d_o_b",DateType(),nullable=True)
                                 ])

# COMMAND ----------

data=[("Vishal",1,24,"Male",date(2000,1,9)),
      ("Akhilesh",2,23,"Male",date(2001,8,31)),
      ("Rahul",3,24,"Male",date(1998,11,2)),
      ("Varun",4,24,"Male",date(2000,4,16))
      ]

# COMMAND ----------

df=spark.createDataFrame(data,sample_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %run /Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/50_split

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(f"sample_table_4")
