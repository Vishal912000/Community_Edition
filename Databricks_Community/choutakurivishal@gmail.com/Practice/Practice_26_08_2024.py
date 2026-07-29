# Databricks notebook source
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

df_schema=StructType(fields=[StructField("raceId",IntegerType(),nullable=True),
                             StructField("year",IntegerType(),nullable=True),
                             StructField("round",IntegerType(),nullable=True),
                             StructField("circuitId",IntegerType(),nullable=True),
                             StructField("name",StringType(),nullable=True),
                             StructField("date",DateType(),nullable=True),
                             StructField("time",StringType(),nullable=True),
                             StructField("url",StringType(),nullable=True)
                             ])

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,schema=df_schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit,col

# COMMAND ----------

df_new=df.withColumn("ingestion_time",current_timestamp())\
         .withColumn("time_stamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
         .withColumnRenamed("raceId","race_id")\
         .withColumnRenamed("circuitId","circuit_id")
        
display(df_new)

# COMMAND ----------

#dropping columns
df_dropped=df_new.drop("date","time","url")
display(df_dropped)

# COMMAND ----------

df_dropped.write.mode("overwrite").format("parquet").save("")

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)
                               ])

json_schema=StructType(fields=[StructField("code",StringType(),True),
                               StructField("dob",DateType(),True),
                               StructField("driverId",IntegerType(),True),
                               StructField("driverRef",StringType(),True),
                               StructField("name",name_schema,True),
                               StructField("nationality",StringType(),True),
                               StructField("url",StringType(),True)
                               ])

# COMMAND ----------

#reading a nested  json file 
json_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=json_schema)
display(json_df)

# COMMAND ----------

json_df_new=json_df.withColumn("full_name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                    .withColumn("ingestion_time",current_timestamp())\
                    .withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("driverRef","driver_ref")\
                    .drop("name","url")

display(json_df_new)

# COMMAND ----------

drivers_pare=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers_final")
display(drivers_pare)
