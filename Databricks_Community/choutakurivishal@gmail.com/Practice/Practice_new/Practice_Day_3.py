# Databricks notebook source
### **New Combination 3**
# 1. Import all the required modules  
# 2. Create a schema (integer, string, double, date)  
# 3. Create a data list  
# 4. Create a data frame using data list and schema  
# 5. Run any notebook  
# 6. Create a delta table using any dataframe  
# 7. Read a CSV file  
# 8. Read a JSON file  
# 9. Perform joins  
# 10. Create a temporary view (global temp view)  
# 11. Perform SQL (insert, select, merge, update, delete)  

# COMMAND ----------

from pyspark.sql.types  import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from datetime import date
from pyspark.sql.functions import sum

# COMMAND ----------

sample_schema=StructType(fields=[StructField("Id",IntegerType(),nullable=True),
                               StructField("Full_name",StringType(),nullable=True),
                               StructField("d_o_b",DateType(),nullable=True)
                               ])

# COMMAND ----------

data=[(1,"Choutakuri Vishal",date(2024,1,9)),
       (2,"Choutakuri Akhilesh",date(2001,8,31)
        )]

# COMMAND ----------

sample_df=spark.createDataFrame(data,sample_schema)
display(sample_df)

# COMMAND ----------

# MAGIC %run /Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/50_split

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

sample_df.write.format("delta").mode("overwrite").saveAsTable(f"sample_table3")
