# Databricks notebook source
# MAGIC %md
# MAGIC #checkpointing :-
# MAGIC
# MAGIC checkpointing is the process of saving the dataframe resultant's data into storage.
# MAGIC
# MAGIC It is one of the performance optimisation technique.
# MAGIC
# MAGIC Though ,It is different from cache ,looks similar in some aspects.
# MAGIC
# MAGIC It boosts the performance for use-cases where particular dataframe gets iterated multiple times.
# MAGIC
# MAGIC Checkpointing writes the data into disk.
# MAGIC
# MAGIC #uses:-
# MAGIC #1.Fault Tolerance :-
# MAGIC Recovers the state of the streaming application after a failure
# MAGIC
# MAGIC #2.Performance Optimisation:-
# MAGIC It optimises the performance by reducing computation.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #
# MAGIC So,Here In the example we will see there are some transformations 
# MAGIC
# MAGIC applied on df_1,df_2,df_3 and df_4 and checkpoint is applied on df_4
# MAGIC
# MAGIC When we are trying to retrieve the data (or) any action is applied on df_8 then the spark computes df_7,df_6,df_5,df_4
# MAGIC
# MAGIC As df_4 is checkpoint df (spark doesn't need to re-compute the df's before checkpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC #syntax:-
# MAGIC
# MAGIC 1.Specify the delta table for checkpointing
# MAGIC
# MAGIC checkpoint_path="/mnt/delta/checkpoints/checkpoint_1"
# MAGIC
# MAGIC 2.Save the dataframe in deltaformat
# MAGIC
# MAGIC df.write.format("delta").mode("overwrite").save(checkpoint_path)
# MAGIC
# MAGIC 3.Verify the delta format checkpoint 
# MAGIC
# MAGIC checkpoint_df=spark.read.format("delta").load("checkpoint_path")
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,DoubleType
from pyspark.sql.functions import col
from datetime import date


# COMMAND ----------

schema=StructType(fields=[StructField("emp_name",StringType(),nullable=True),
                          StructField("emp_id",IntegerType(),nullable=True),
                          StructField("emp_salary",DoubleType(),nullable=True),
                          StructField("emp_doj",DateType(),nullable=True),
                          StructField("emp_dob",DateType(),nullable=True)
                          ])

data=[("Vishal",1,150000.55,date(2025,6,28),date(2000,1,9)),
      ("Akhilesh",2,100000.30,date(2026,8,9),date(2001,8,31)),
      ("Shreesha",3,2000000.30,date(2027,5,6),date(2005,5,11))
      ]

# COMMAND ----------

df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("checkpoint_path_1")
