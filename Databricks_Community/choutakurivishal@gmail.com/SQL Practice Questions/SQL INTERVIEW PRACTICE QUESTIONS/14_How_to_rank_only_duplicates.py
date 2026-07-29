# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# (In Databricks, SparkSession is usually already created)

# Define schema
schema = StructType([
    StructField("id", StringType(), True)
])

# Define data
data = [
    ("a",),
    ("a",),
    ("b",),
    ("c",),
    ("c",),
    ("c",),
    ("d",),
    ("d",)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
display(df)  # 'display' is a Databricks function, better for viewing tables


# COMMAND ----------


