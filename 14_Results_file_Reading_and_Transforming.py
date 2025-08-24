# Databricks notebook source
from pyspark.sql.types import IntegerType,StringType,DateType,StructType,StructField

# COMMAND ----------

results_schema=StructType(fields=[StructField("constructorId",IntegerType(),True),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("fastestLap",StringType(),True),
                                   StructField("fastestLapSpeed",StringType(),True),
                                   StructField("fastestLapTime",StringType(),True),
                                   StructField("grid",IntegerType(),True),
                                   StructField("laps",IntegerType(),True),
                                   StructField("milliseconds",StringType(),True),
                                   StructField("number",IntegerType(),True),
                                   StructField("points",IntegerType(),True),
                                   StructField("position",StringType(),True),
                                   StructField("positionOrder",IntegerType(),True),
                                   StructField("positionText",StringType(),True),
                                   StructField("raceId",IntegerType(),True),
                                   StructField("rank",StringType(),True),
                                   StructField("resultId",IntegerType(),True),
                                   StructField("statusId",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   ])

# COMMAND ----------

results_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results.json",header=True,schema=results_schema)
#display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

results_df_with=results_df.withColumn("ingestion_time", current_timestamp())\
                          .drop(col('statusId'))\
                          .withColumnRenamed("constructorId","constructor_id")\
                          .withColumnRenamed('driverId','driver_id')\
                          .withColumnRenamed('fastestlap','fastest_lap')\
                          .withColumnRenamed('fastestlapspeed','fastest_lap_speed')\
                          .withColumnRenamed('fastestlaptime','fastest_lap_time')\
                          .withColumnRenamed('positionorder','position_order')\
                          .withColumnRenamed('positiontext','position_text')\
                          .withColumnRenamed('resultId','result_id')\
                          .withColumnRenamed('raceId','race_id')

# COMMAND ----------

results_df_with.write.format("parquet").mode("overwrite").partitionBy('race_id').save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results_final")

# COMMAND ----------

results=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results_final/race_id=1")
display(results)