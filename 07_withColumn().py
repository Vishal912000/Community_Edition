# Databricks notebook source
# MAGIC %md
# MAGIC withColumn():-
# MAGIC
# MAGIC withColumn() is used to add a new column to an existing dataframe or replace the existing column that has a same name in a dataframe
# MAGIC
# MAGIC Syntax:-new_df=df.withColumn("new_col_name",df.col_1 * 2)
# MAGIC
# MAGIC We are having 2 parameters in the withColumn , 1st one is column name and 2nd one is condtion for the new column which is being created 
# MAGIC
# MAGIC and the condition should be based on the same dataframe only, else it will throw error
# MAGIC
# MAGIC

# COMMAND ----------

while 2>1:
    input("my")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_1=df.select(col("circuitId").alias("circuit_id"),
               col("circuitRef").alias("circuit_ref"),
               col("name"),
               col("location"),
               col("country"),
               col("lat").alias("latitude"),
               col("lng").alias("longitude"),
               col("alt").alias("altitude")
               )

# COMMAND ----------

df_with=df_1.withColumn("latitude and  longitude",df_1.latitude +df_1.longitude )
display(df_with)

# COMMAND ----------

# we are importing current_timestamp function for using it withColumn
df_final=df_1.withColumn("ingestion_date",current_timestamp())
display(df_final)

# COMMAND ----------

# MAGIC %md 
# MAGIC Here we are importing lit function so we can use the value literals in our with columns
# MAGIC
# MAGIC Syntax:-
# MAGIC lit(col) -->It creates a column of literal value or a string 

# COMMAND ----------

#multiple with columns
df_with_2=df_1.withColumn("doop",df_1.circuit_id * 2)\
              .withColumn("doop2",df_1.circuit_id * 5)\
              .withColumn("environment",lit("production"))
display(df_with_2)