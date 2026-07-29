# Databricks notebook source
df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,DateType,StructField,StructType,BooleanType

# COMMAND ----------

schema_df=StructType(fields=[StructField("race_id",StringType(),nullable=True),
                             StructField("year",IntegerType(),nullable=True),
                             StructField("round",IntegerType(),nullable=True),
                             StructField("circuitid",IntegerType(),nullable=True),
                             StructField("name",StringType(),nullable=True),
                             StructField("date",DateType(),nullable=True),
                             StructField("time",StringType(),nullable=True),
                             StructField("url",StringType(),nullable=True)
                             ])

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,schema=schema_df)

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_table")



# COMMAND ----------

df=spark.sql(f"""select * from df_table""")

# COMMAND ----------

df1=df.select("race_id","year")

# COMMAND ----------

schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                          StructField("number",IntegerType(),nullable=True),
                          StructField("age",IntegerType(),nullable=True),
                          StructField("sex",StringType(),nullable=True),
                          StructField("Married",BooleanType(),nullable=True
                                      )])

# COMMAND ----------

schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                          StructField("age",IntegerType(),nullable=True),
                          StructField("sex",StringType(),nullable=True),
                          ])
