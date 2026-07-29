# Databricks notebook source
# MAGIC %md Here we are reading a complete csv and a json folder and transforming the file 

# COMMAND ----------

df=spark.read.format('csv').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/lap_times',inferSchema=True)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType

# COMMAND ----------

laptimes_schema=StructType(fields=[StructField('raceid',IntegerType(),True),
                                   StructField('driverid',IntegerType(),True),
                                   StructField('lap',IntegerType(),True),
                                   StructField('position',IntegerType(),True),
                                   StructField('time',StringType(),True),
                                   StructField('milliseconds',IntegerType(),True)
                                   ])

# COMMAND ----------

laptimes_df=spark.read.format('csv').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/lap_times',schema=laptimes_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

laptimes_1=laptimes_df.withColumn('ingestion_time',current_timestamp())\

# COMMAND ----------

laptimes_2=laptimes_1.select(col('raceid').alias('race_id'),
                             col('driverid').alias('driver_id'),
                             col('lap'),
                             col('position'),
                             col('time'),
                             col('milliseconds'),
                             col('ingestion_time')
                             )
display(laptimes_2)

# COMMAND ----------

laptimes_2.write.format('parquet').mode('overwrite').save('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/lap_times_final',)
