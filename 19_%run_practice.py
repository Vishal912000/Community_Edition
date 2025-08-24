# Databricks notebook source
# MAGIC %run /Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/18_%run_detailed

# COMMAND ----------

df=spark.read.format('csv').load(f'{raw_file_name}/circuits.csv',header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType,DateType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField('circuitId',IntegerType(),nullable=True),
                          StructField('circuitRef',StringType(),nullable=True),
                          StructField('name',StringType(),nullable=True),
                          StructField('location',StringType(),nullable=True),
                          StructField('country',StringType(),nullable=True),
                          StructField('lat',FloatType(),nullable=True),
                          StructField('lng',FloatType(),nullable=True),
                          StructField('alt',IntegerType(),nullable=True),
                          StructField('url',StringType(),nullable=True)
                          ])

# COMMAND ----------

df_circuits=spark.read.format('csv').load(f'{raw_file_name}/circuits.csv',header=True,schema=circuits_schema)

# So here we can see as we are using run magic command we are able to use  the raw_file_name variable name in this notebopok

# COMMAND ----------

df_circuits_with=add_timestamp(df_circuits)

# So here we can see as we are using run magic command we are able to run the add timestamp function in this notebopok