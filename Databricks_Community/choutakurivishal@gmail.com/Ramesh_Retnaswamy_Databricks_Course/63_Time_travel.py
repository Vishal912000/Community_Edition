# Databricks notebook source
# MAGIC %md
# MAGIC https://www.youtube.com/watch?v=3av7ctZ1uoo

# COMMAND ----------

# MAGIC %md
# MAGIC #Time Travel Feature in Delta tables :-
# MAGIC Time travel in delta lake enables users to query ,restore and audit historical versions from a delta table
# MAGIC
# MAGIC This is a powerful feature that allows you to track changes ,compare data states and recover accidentally deleted or modified data
# MAGIC
# MAGIC Delta lake time travel ensures that every operation you carry out when writing to a  delta table is versioned automatically
# MAGIC
# MAGIC There are 2 methods you can access various versions of data
# MAGIC 1) Applying a timestamp
# MAGIC
# MAGIC 2) Using a versioning system 
# MAGIC
# MAGIC #Syntax:
# MAGIC select * from table_name version as of version_number
# MAGIC
# MAGIC select * from table_name timestamp as of "timestamp"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import col
from datetime import date 

# COMMAND ----------

sample_schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                                 StructField("age",IntegerType(),nullable=True),
                                 StructField("sex",StringType(),nullable=True),
                                 StructField("D_o_BB",DateType(),nullable=True)
                                 ])

# COMMAND ----------

data=[("Vishal",25,"Male",date(2000,1,9)),
        ("Akhilesh",24,"Male",date(2001,8,31)),
       ("Shreesha",20,"Female",date(2005,5,11))
       ]

# COMMAND ----------

sample_df=spark.createDataFrame(data,sample_schema)
display(sample_df)

# COMMAND ----------

sample_df.write.format("delta").saveAsTable(f"sample_time_travel_")

# COMMAND ----------

sample_df.write.format("delta").mode("append").saveAsTable(f"sample_time_travel_")

#Here i am appending the same data to the same table again so we can check whether 2 versions are created using time travel,so there will be duplicates created 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample_time_travel_

# COMMAND ----------

# MAGIC %md
# MAGIC #describe history

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history sample_time_travel_
# MAGIC
# MAGIC --Using this we can find all the versions of the  delta table 

# COMMAND ----------

# MAGIC %sql
# MAGIC --So,Here I am using time travel and getting the version 0 of the delta table
# MAGIC select * from sample_time_travel_ version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC --So,Here I am using time travel and getting the version 0 of the delta table
# MAGIC select * from sample_time_travel_ timestamp as of "2025-01-16T10:49:59.000+00:00"

# COMMAND ----------

# MAGIC %md
# MAGIC #restore 
# MAGIC
# MAGIC restore command is used to restore the delta table with previous versions if we have updated,deleted or inserted anything incorrect
# MAGIC
# MAGIC #syntax:
# MAGIC restore table table_name to version as of 3
# MAGIC
# MAGIC restore table table_name to timestamp as of '2025-01-15T17:26:39.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table sample_time_travel_ to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history sample_time_travel_
