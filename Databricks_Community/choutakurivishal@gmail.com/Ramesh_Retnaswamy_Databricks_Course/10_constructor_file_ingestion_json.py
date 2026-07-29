# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

constructor_schema=StructType(fields=[StructField("constructorId",IntegerType(),nullable=True),
                                      StructField("constructorRef",StringType(),True),
                                      StructField("name",StringType(),True),
                                      StructField("nationality",StringType(),True),
                                      StructField("url",StringType(),True)
                                      ])

# COMMAND ----------

constructor_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructors.json",header=True,schema=constructor_schema)
display(constructor_df)

# COMMAND ----------

constructor_dropped_df=constructor_df.drop(constructor_df.url)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_df_final=constructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
                                      .withColumnRenamed('constructorRef','constructor_ref')\
                                      .withColumn('ingestion_time',current_timestamp())

# COMMAND ----------

constructor_df_final.write.mode('overwrite').format('parquet').save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructor_final")

# COMMAND ----------

constr=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructor_final")
display(constr)
