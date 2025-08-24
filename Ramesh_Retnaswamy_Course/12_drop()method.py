# Databricks notebook source
# MAGIC %md 
# MAGIC drop() method :drop() method is used to remove a specific column from a data frame
# MAGIC
# MAGIC It returns a new dataframe by removing the specific column
# MAGIC
# MAGIC Syntax:-
# MAGIC
# MAGIC df_new=df.drop("column_name)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

constructor_schema=StructType(fields=[StructField("constructorId",IntegerType(),nullable=True),
                                      StructField("constructorRef",StringType(),nullable=True),
                                      StructField("name",StringType(),True),
                                      StructField("nationality",StringType(),True),
                                      StructField("url",StringType(),True)
                                      ])

# COMMAND ----------

constructor_df=spark.read.format('json').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructors.json',header=True,schema=constructor_schema)

# COMMAND ----------

constructor_dropped_df=constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC                                                                   OR

# COMMAND ----------

constructor_dropped_df_2=constructor_df.drop(constructor_df.url)

# COMMAND ----------

# dropping multiple columns 

df_dropped_multi=constructor_df.drop(constructor_df.url,constructor_df.name)

#                                or 

df_dropped_multi_1=constructor_df.drop('url','name')
# we can use all ways just as select and also can chain with other dataframe apis
