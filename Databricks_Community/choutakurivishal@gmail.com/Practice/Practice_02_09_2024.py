# Databricks notebook source
dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,DoubleType

# COMMAND ----------

inner_schema=StructType(fields=[StructField("forename",StringType(),nullable=True),
                                StructField("surname",StringType(),nullable=True)
                                ])

json_schema=StructType(fields=[StructField("code",StringType(),nullable=True),
                               StructField("dob",DateType(),nullable=True),
                               StructField("driverId",IntegerType(),nullable=True),
                               StructField("driverRef",StringType(),nullable=True),
                               StructField("name",inner_schema,nullable=True),
                               StructField("nationality",StringType(),nullable=True),
                               StructField("number",StringType(),nullable=True),
                               StructField("url",StringType(),nullable=True)
                               ])

# COMMAND ----------

drivers_df=spark.read.format("json").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json",header=True,schema=json_schema)
display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,to_timestamp,concat,lit

# COMMAND ----------

drivers_df_2=drivers_df.withColumn("ingestion_time",current_timestamp())\
                       .withColumn("full_name",concat(col('name.forename'),lit(' '),col('name.surname')))\
                       .withColumn("dob",to_timestamp(col('dob'),'yyyy-MM-dd HH:MM:SS'))\
                       .drop(col("name"),col("url"))
display(drivers_df_2)

# COMMAND ----------

drivers_df_3=drivers_df_2.select(col("code"),
                                 col("dob"),
                                 col("driverId").alias("driver_id"),
                                 col("driverRef").alias("driver_ref"),
                                 col("full_name"),
                                 col("nationality"),
                                 col("number"),
                                 col("ingestion_time")
                                 )

# COMMAND ----------

drivers_df_3.write.format("parquet").mode("overwrite").save("file_path")
