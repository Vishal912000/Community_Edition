# Databricks notebook source
# MAGIC %md
# MAGIC #cache():-
# MAGIC cache is a programming mechanism through which it is possible to store the data temporarily in the memory across the worker nodes in a cluster
# MAGIC
# MAGIC #When to use cache?
# MAGIC When it is desired to store the data only in the memory ,then cache can be used.
# MAGIC
# MAGIC #why to use cache?
# MAGIC As per spark architecture,It is already performing "In-Memory Computation,then question arises why to use cache and persist at all
# MAGIC
# MAGIC We need to understand about transformations and actions ,since transformations are lazily evaluated in spark,when some transformations are created only the logical steps of those are to be kept under "DAG"
# MAGIC
# MAGIC But,The execution of those transformations would start only when an action is called.
# MAGIC
# MAGIC If there are 1000's of transformations and then and action is called ,then action will trigger the actual data processing (starting from 1st transformation all transformations will be applied one by one and finally the action will be applied)
# MAGIC
# MAGIC So,If we want to store some transformation result without executing an Action 
# MAGIC
# MAGIC We need to use cache and persist,cache is used for performance optimisation as it reduces re-computing
# MAGIC
# MAGIC #syntax:-
# MAGIC
# MAGIC df.cache()
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType
from pyspark.sql.functions import col,lit
from datetime import date

# COMMAND ----------

sample_schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                                StructField("age",IntegerType(),nullable=True),
                                StructField("sex",StringType(),nullable=True),
                                StructField("height",DoubleType(),nullable=True),
                                StructField("dob",DateType(),nullable=True)
                                ])


data=[("Vishal",24,"Male",5.11,date(2000,1,9)),
      ("Akhilesh",23,"Male",5.8,date(2001,8,31)),
      ("Shreesha",19,"Female",5.9,date(2005,5,11))]

# COMMAND ----------

df=spark.createDataFrame(data,sample_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

df_1=df.withColumn("first_name",lit("c"))
display(df_1)

# COMMAND ----------

df_2=df.withColumn("salary",lit(100000))

# COMMAND ----------

https://oindrila-chakraborty88.medium.com/cache-vs-persist-in-databricks-774b5faaa430
