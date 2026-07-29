# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC We are reading a multiline json file in this notebook,
# MAGIC
# MAGIC So we need to use multiline keyword as True in the load parenthesis

# COMMAND ----------

#This is sample for reading the schema of the json file i am using infer schema 
#df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/pit_stops-1.json",header=True,inferSchema=True,multiLine=True)
#display(df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

pits_schema=StructType(fields=[StructField("driverId",IntegerType(),nullable=True),
                               StructField('duration',StringType(),nullable=True),
                               StructField('lap',IntegerType(),nullable=True),
                               StructField('milliseconds',IntegerType(),nullable=True),
                               StructField('raceId',IntegerType(),nullable=True),
                               StructField('stop',IntegerType(),nullable=True),
                               StructField('time',StringType(),nullable=True)
                       ])

# COMMAND ----------

pits_df=spark.read.format('json').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/pit_stops-1.json',header=True,multiLine=True,schema=pits_schema)
display(pits_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

pits_df_1=pits_df.withColumn('ingestion_time',current_timestamp())\
                 .withColumnRenamed('driverId','driver_id')\
                 .withColumnRenamed('raceId','race_id')

# COMMAND ----------

pits_df_1.write.format('parquet').mode('overwrite').save('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/pit_stops_final')
