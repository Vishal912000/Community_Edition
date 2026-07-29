-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
-- MAGIC from datetime import date
-- MAGIC
-- MAGIC
-- MAGIC schema=StructType(fields=[StructField("cx_name",StringType(),nullable=True),
-- MAGIC                           StructField("order_date",DateType(),nullable=True),
-- MAGIC                           StructField("order_amt",IntegerType(),nullable=True),
-- MAGIC                           StructField("gender",StringType(),nullable=True)
-- MAGIC                             ])
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=[("shilpa",date(2025,1,12),100,"F"),
-- MAGIC       ("Vishal",date(2025,1,15),999,"M"),
-- MAGIC       ("SHILPA",date(2025,2,9),9999,"F"),
-- MAGIC       ("Shilpa",date(2025,3,6),88,"F")]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.createDataFrame(data,schema)
-- MAGIC display(df)
-- MAGIC df.createOrReplaceTempView("df")

-- COMMAND ----------

select * from df 
where lower(cx_name)='shilpa'
