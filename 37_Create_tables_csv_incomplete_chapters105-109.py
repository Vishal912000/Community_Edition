# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,BooleanType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                          StructField("circuitRef",StringType(),nullable=True),
                          StructField("name",StringType(),nullable=True),
                          StructField("location",StringType(),nullable=True),
                          StructField("country",StringType(),nullable=True),
                          StructField("lat",DoubleType(),nullable=True),
                          StructField("lng",DoubleType(),nullable=True),
                          StructField("alt",IntegerType(),nullable=True),
                          StructField("url",StringType(),nullable=True)
                          ])

# COMMAND ----------

circuits_df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=circuits_schema)

# COMMAND ----------

circuits_df.createOrReplaceTempView("circuits_table")

# COMMAND ----------

circuits=spark.sql(f"""select * from circuits_table""")