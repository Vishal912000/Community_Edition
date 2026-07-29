# Databricks notebook source
### **New Combination 5**
# 1. Import all the required modules  
# 2. Create a schema (integer, string, double, date)  
# 3. Create a data list  
# 4. Create a data frame using data list and schema  
# 5. Run any notebook  
# 6. Create a delta table using any dataframe  
# 7. Read a CSV file  
# 8. Read a JSON file  
# 9. Perform rank functions  
# 10. Create widgets  
# 11. Perform SQL (insert, select, merge, update, delete)  

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField,DoubleType,DateType
from pyspark.sql.functions import sum,col
from datetime import date 

# COMMAND ----------

sample_schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                                 StructField("age",IntegerType(),nullable=True),
                                 StructField("sex",StringType(),nullable=True),
                                 StructField("d_o_b",DateType(),nullable=True)
                                 ])

# COMMAND ----------

sample_data=[("Vishal",26,"M",date(2000,1,9)),
             ("Akhilesh",25,"M",date(2001,8,31)),
             ("Shreesha",21,"F",date(2005,5,11))]

# COMMAND ----------

# DBTITLE 1,Create DataFrame with correct argument order
df = spark.createDataFrame(sample_data, sample_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("sample_table_05")

# COMMAND ----------

df_2=spark.sql("select * from sample_table_05")
display(df_2)

# COMMAND ----------

spark.read.format("csv").load("file_name",header=True,inferSchema=True,)
