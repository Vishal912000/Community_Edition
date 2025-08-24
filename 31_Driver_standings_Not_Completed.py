# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
schema=StructType(fields=[StructField("name",IntegerType(),nullable=True),
                          StructField("age",IntegerType(),nullable=True),
                          StructField("sex",StringType(),nullable=True),
                          StructField("religion",StringType(),nullable=True)
                          ])

#created as dummy for practice

# COMMAND ----------

race_results=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results_final")

# COMMAND ----------

display(race_results)

# COMMAND ----------

races_filter=race_results.filter("race_year=2020 and driver_name='Daniil Kvyat'")
display(races_filter)

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

races_reults_group=race_results.groupBy("race_year","driver_name","driver_funtionality").agg(count("driver_name"))
display(races_reults_group)

# COMMAND ----------



# COMMAND ----------

