# Databricks notebook source
https://www.youtube.com/watch?v=xUsY2jWQa1w&list=PLgCGwQ1v9hKPbIl6zmxkK2LJA2GbSl5lF

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,DateType,BooleanType,StructType,StructField
from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateTables").getOrCreate()

# Define schema for Table 1
schema_table1 = StructType([
    StructField("col1", IntegerType(), True)
])

# Define schema for Table 2
schema_table2 = StructType([
    StructField("col2", IntegerType(), True)
])

# Create data for Table 1
data_table1 = [(1,), (1,), (1,), (2,), (3,), (3,), (3,)]

# Create data for Table 2
data_table2 = [(1,), (1,), (2,), (2,), (4,), (None,),(None,)]

# Create DataFrames
table1 = spark.createDataFrame(data_table1, schema_table1)
table2 = spark.createDataFrame(data_table2, schema_table2)

# Show the tables
print("Table 1:")
table1.show()

print("Table 2:")
table2.show()
table1.createOrReplaceTempView("table1")
table2.createOrReplaceTempView("table2")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table2

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Inner join
# MAGIC select * from table1
# MAGIC join table2

# COMMAND ----------

---Left join
select * from table1
join table2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For joining we need to use th on condition ,if we don't use on condition then it will perform cross join
# MAGIC
# MAGIC 🔹 **Inner Join**  
# MAGIC Returns only matching rows.  
# MAGIC **Formula:**  
# MAGIC > Inner Join = Only Matching Rows (intersection of two tables)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🔹 **Left Join**  
# MAGIC Returns all rows from the left table + matching rows from the right table.  
# MAGIC **Formula:**  
# MAGIC > Left Join = Inner Join + Unmatched Left Table Rows (right side NULL)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🔹 **Right Join**  
# MAGIC Returns all rows from the right table + matching rows from the left table.  
# MAGIC **Formula:**  
# MAGIC > Right Join = Inner Join + Unmatched Right Table Rows (left side NULL)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🔹 **Full Outer Join**  
# MAGIC Returns all rows from both tables (matched + unmatched).  
# MAGIC **Formula:**  
# MAGIC > Full Outer Join = Inner Join + Unmatched Left Rows + Unmatched Right Rows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🔹 **Natural Join**  
# MAGIC Automatically joins tables by **columns with the same name** and matches values.  
# MAGIC **Formula:**  
# MAGIC > Natural Join = Inner Join **based on common columns** (no need to specify ON condition)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🔹 **Cross Join**  
# MAGIC Returns **Cartesian Product** → each row of the first table joins with **every row** of the second table.  
# MAGIC **Formula:**  
# MAGIC > Cross Join = (Number of Rows in Table 1) × (Number of Rows in Table 2)
# MAGIC
# MAGIC ---
# MAGIC  
