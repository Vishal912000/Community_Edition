# Databricks notebook source
# MAGIC %md
# MAGIC #Case:-
# MAGIC case statement is a conditional statement that goes through multiple conditions in a sql query and returns a value based on first condition which is true(like an if-else condition) 
# MAGIC
# MAGIC If no conditions are true then it returns value in else clause
# MAGIC
# MAGIC If else clause is not available then it returns null
# MAGIC
# MAGIC #Syntax:-
# MAGIC
# MAGIC select col_1,
# MAGIC         case when col_2 > 4 then col_2 + 1
# MAGIC              when col_2 > 5 then col_2 + 2
# MAGIC              when col_2 > 6 then col_2 + 4
# MAGIC         else col_2 + 5
# MAGIC         end as new_col_name
# MAGIC         from table_name
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import col
from datetime import date

# COMMAND ----------

sample_schema=StructType(fields=[StructField("emp_name",StringType(),nullable=True),
                                 StructField("emp_id",IntegerType(),nullable=True),
                                 StructField("emp_performance",IntegerType(),nullable=True),
                                 StructField("emp_dob",DateType(),nullable=True)
                                 ])
data=[("Vishal",1,10,date(2000,1,9)),
      ("Akhilesh",2,9,date(2001,8,31)),
      ("Shreesha",3,9,date(2005,5,11)),
      ("juju",4,7,date(2025,1,1))
      ]

# COMMAND ----------

df=spark.createDataFrame(data,sample_schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, case when emp_performance =10 then emp_performance + 5
# MAGIC                when emp_performance =9  then emp_performance + 3
# MAGIC                else emp_performance -1
# MAGIC                end as emp_future_performace
# MAGIC             from df_table
# MAGIC --So here we are giving a case condition based on emp_performance and deriving the future performance of the employee 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,case when emp_performance >= 10 then 150000
# MAGIC                when emp_performance = 9 then 100000
# MAGIC                else 60000
# MAGIC                end as emp_salary
# MAGIC                from df_table
# MAGIC
# MAGIC --Here we are creating a new column using case condition and deriving the emp salary based on their performance ,before we have created a column using the refernce of emp_performance(by calculating emp_performance + 1 ) 
# MAGIC
# MAGIC --Now,we are just giving values directly 
