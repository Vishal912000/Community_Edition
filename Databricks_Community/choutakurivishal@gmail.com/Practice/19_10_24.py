# Databricks notebook source
from pyspark.sql.types import IntegerType,StringType,DateType,StructType,StructField

# COMMAND ----------

schema_df=StructType(fields=[StructField("race_id",IntegerType(),nullable=True),
                             StructField("year",IntegerType(),nullable=True),
                             StructField("round",IntegerType(),nullable=True),
                             StructField("circuitId",IntegerType(),nullable=True),
                             StructField("name",StringType(),nullable=True),
                             StructField("date",DateType(),nullable=True),
                             StructField("time",StringType(),nullable=True),
                             StructField("url",StringType(),nullable=True)
                             ])

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,schema=schema_df)

# COMMAND ----------

df_select=df.select(df.race_id.alias("race_id"),
                    df.year.alias("race_year"),
                    df.round,
                    df.circuitId.alias("circuit_id"),
                    df.name.alias("race_name"),
                    df.time.alias("race_time"),
                    df.url)

# COMMAND ----------

df_withCoulumnRenamed=df.withColumnRenamed("race_id","race")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df_withColumn=df.withColumn("environment",lit("Production"))
