# Databricks notebook source
# MAGIC %md Here we are reading a multi line json folder which contains two json files 

# COMMAND ----------

df=spark.read.format('json').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/qualifying',inferSchema=True,multiLine=True)
#display(df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType,DateType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

qualifying_schema=StructType(fields=[StructField('constructorId',IntegerType(),True),
                                     StructField('driverId',IntegerType(),True),
                                     StructField('number',IntegerType(),True),
                                     StructField('position',IntegerType(),True),
                                     StructField('q1',StringType(),True),
                                     StructField('q2',StringType(),True),
                                     StructField('q3',StringType(),True),
                                     StructField('qualifyId',IntegerType(),True),
                                     StructField('raceId',IntegerType(),True)
                             ])

# COMMAND ----------

qualifying_df=spark.read.format('json').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/qualifying',schema=qualifying_schema,multiLine=True)

#

# COMMAND ----------

qualifying_df_1=qualifying_df.withColumn('ingestion_time',current_timestamp())\
             .withColumnRenamed('constructorId','constructor_id')\
             .withColumnRenamed('driverId','driver_id')\
             .withColumnRenamed('raceId','race_id')

#########################   OR      ###################################
qualifying_df_2=qualifying_df.withColumn('ingestion_date',current_timestamp())\
                             .select(col('constructorId').alias('constructor_id'),
                                     col('driverId').alias('driver_id'),
                                     col('number'),
                                     col('position'),
                                     col('q1'),
                                     col('q2'),
                                     col('q3'),
                                     col('qualifyId').alias('qualify_id'),
                                     col('raceId').alias('race_id'),
                                     col('ingestion_date')
                                     )
# display(qualifying_df_1)
# display(qualifying_df_2)

# COMMAND ----------

qualifying_df_1.write.format('parquet').mode('overwrite').save('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/qualifying_final')