# Databricks notebook source
from pyspark.sql.functions import col,lit,current_timestamp,to_timestamp,concat
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,inferSchema=True)
#display(df)

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),nullable=True),
                                StructField("year",IntegerType(),nullable=True),
                                StructField("round",IntegerType(),nullable=True),
                                StructField("circuitId",IntegerType(),nullable=True),
                                StructField("name",StringType(),nullable=True),
                                StructField("date",DateType(),nullable=True),
                                StructField("time",StringType(),nullable=True),
                                StructField("url",StringType(),nullable=True)
                                ])

# COMMAND ----------

races_df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races.csv",header=True,schema=races_schema)
display(races_df)

# COMMAND ----------

races_df_1=races_df.select(col("raceId").alias("race_id"),
                           col("year"),
                           col("round"),
                           col("circuitId").alias("circuit_id"),
                           col("name"),
                           col("date"),
                           col("time"))

# COMMAND ----------

races_df_2=races_df_1.withColumn("ingestion_time",current_timestamp())\
                     .withColumn("race_timestamp",to_timestamp (concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))
display(races_df_2)

# COMMAND ----------

races_df_3=races_df_2.select(col('race_id'),
                             col('year'),
                             col('round'),
                             col('circuit_id'),
                             col('name'),
                             col('race_timestamp'),
                             col('ingestion_time'))

# COMMAND ----------

races_df_final=races_df_3.write.mode("overwrite").format("parquet").save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_final")

# COMMAND ----------

# MAGIC %md 
# MAGIC partitionBy():partition by is used to divide the the data into smaller partitions 
# MAGIC
# MAGIC Here,we are Partitioning Data When Writing to Storage ,using partitionBy() keyword 
# MAGIC
# MAGIC So when we use partitionBy in writer api it splits the files based on the column given in partitionBy()
# MAGIC
# MAGIC Ex:-Here we are using partitionBy("year") so we can see the data is written as sub-files based on year
# MAGIC
# MAGIC You can check how data is stored in dbfs file path
# MAGIC
# MAGIC

# COMMAND ----------

races_final_part=races_df_3.write.format("parquet").mode('overwrite').partitionBy("year").save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_fi")

# COMMAND ----------

race=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_fi/year=1950")
display(race)
