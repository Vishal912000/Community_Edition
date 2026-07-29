# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateScenario2Tables").getOrCreate()

# Define schema for Table 3
schema_table3 = StructType([
    StructField("col1", IntegerType(), True)
])

# Define schema for Table 4
schema_table4 = StructType([
    StructField("col2", IntegerType(), True)
])

# Create data for Table 3
data_table3 = [(1,), (1,), (1,), (1,), (None,), (None,)]

# Create data for Table 4
data_table4 = [(1,), (1,), (2,), (None,), (None,)]

# Create DataFrames
table3 = spark.createDataFrame(data_table3, schema_table3)
table4 = spark.createDataFrame(data_table4, schema_table4)

# Show the tables
print("Table 3:")
table3.show()

print("Table 4:")
table4.show()

