# Databricks notebook source
# MAGIC %md
# MAGIC Dataframe writer API is used to write the dataframe contents in various external storage systems
# MAGIC
# MAGIC It allows you to specify the file format,options and mode in which the data should be written
# MAGIC
# MAGIC ------>File Formats supported are:-
# MAGIC                                     1. "parquet"
# MAGIC                                     2. "csv"
# MAGIC                                     3. "json"
# MAGIC                                     4. "avro"
# MAGIC                                     5. "orc"
# MAGIC                                     6. "delta" (for Delta Lake)
# MAGIC
# MAGIC
# MAGIC ------>Options :
# MAGIC             1.header
# MAGIC             2.
# MAGIC
# MAGIC
# MAGIC ------>Modes:
# MAGIC             1.append:: Appends the data to the existing data.
# MAGIC             2.overwrite:  Overwrites the existing data.
# MAGIC             3.ignore :  Ignores the operation if data already exists.
# MAGIC             4.error :  Throws an error if data already exists (default behavior)
# MAGIC
# MAGIC
# MAGIC Syntax:-df.write.format("csv").mode("overwrite").save("file_path")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType
from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True)
display(df)

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),nullable=True),
                          StructField("circuitRef",StringType(),nullable=True),
                          StructField("name",StringType(),nullable=True),
                          StructField("location",StringType(),nullable=True),
                          StructField("country",StringType(),nullable=True),
                          StructField("lat",DoubleType(),nullable=True),
                          StructField("lng",DoubleType(),nullable=True),
                          StructField("alt",DoubleType(),nullable=True),
                          StructField("url",StringType(),nullable=True)
                          ])           

# COMMAND ----------

circuits_df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,schema=circuits_schema)

# COMMAND ----------

circuits_df_1=circuits_df.select(col("circuitId").alias("circuit_id"),
                                      col("circuitRef").alias("circuit_ref"),
                                      col("name"),
                                      col("location"),
                                      col("country"),
                                      col("lat").alias("latitude"),
                                      col("lng").alias("longitude"),
                                      col("alt").alias("altitude")
                                      )

# COMMAND ----------

circuits_df_2=circuits_df_1.withColumn("ingestion_date",current_timestamp())\
                           .withColumn("environment",lit("production"))
display(circuits_df_2)

# COMMAND ----------

circuits_final=circuits_df_2.write.mode("overwrite").format("parquet").save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final")

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final")
display(df)
