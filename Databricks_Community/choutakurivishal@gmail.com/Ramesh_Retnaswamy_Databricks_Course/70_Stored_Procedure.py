# Databricks notebook source
# MAGIC %md
# MAGIC #Stored Procedure :-
# MAGIC A stored procedure is a group of SQL statements,
# MAGIC
# MAGIC If you have a situation ,where you need to re use the query over and over again,you can save that specific query as a stored procedure and call by its name 
# MAGIC
# MAGIC It is designed to perform a specific task such as querying data,updating records,or executing a set of operations and can be invoked multiple times
# MAGIC
# MAGIC #syntax:
# MAGIC
# MAGIC create procedure procedure_name(
# MAGIC   @country varchar(50)
# MAGIC   @in age int
# MAGIC )
# MAGIC
# MAGIC begin 
# MAGIC select country_name,age_of_employee,emp_id from employee
# MAGIC where country_name=country and age_of_employee=age;
# MAGIC
# MAGIC select count(employee_id) from employee where age=@age;
# MAGIC
# MAGIC end 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType,StringType,DateType,DoubleType,StructType,StructField

# COMMAND ----------

sample_schema=StructType(fields=[StructField("emp_name",StringType(),nullable=True),
                                 StructField("emp_age",IntegerType(),nullable=True),
                                 StructField("emp_sal",IntegerType(),nullable=True),
                                 StructField("country_name",StringType(),nullable=True)
                                 ])

sample_data=[("Vishal",25,150000,"India"),
             ("Akhilesh",24,100000,"India"),
             ("Shreesha",23,800000,"India"),
             ("Rahul",26,100000,"USA"),
             ("Sriram",26,100000,"USA")
             ]

# COMMAND ----------

df=spark.createDataFrame(sample_data,sample_schema)

# COMMAND ----------

df.createOrReplaceTempView("df_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Now,we are creating a stored procedure and stored procedure is not supported in databricks sql 
# MAGIC
# MAGIC DELIMETER $$
# MAGIC create procedure emp_procedure(
# MAGIC   in country varchar(50),
# MAGIC   in age int
# MAGIC )
# MAGIC begin 
# MAGIC
# MAGIC select emp_name,emp_age,emp_sal from df_table
# MAGIC where emp_age=age and country_name=country;
# MAGIC
# MAGIC select count(emp_name) from df_table
# MAGIC where emp_age>age;
# MAGIC
# MAGIC END$$
# MAGIC DELIMITER;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE PROCEDURE emp_procedure(
# MAGIC     IN country VARCHAR(50), -- Input parameter for country
# MAGIC     IN age INT -- Input parameter for age
# MAGIC )
# MAGIC BEGIN
# MAGIC     -- First query: Fetch employee details based on age and country
# MAGIC     SELECT emp_name, emp_age, emp_sal 
# MAGIC     FROM df_table
# MAGIC     WHERE emp_age = age AND country_name = country;
# MAGIC
# MAGIC     -- Second query: Count employees with age greater than the input parameter
# MAGIC     SELECT COUNT(emp_name) AS employee_count 
# MAGIC     FROM df_table
# MAGIC     WHERE emp_age > age;
# MAGIC END
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE PROCEDURE emp_procedure (
# MAGIC     country STRING, -- Input parameter for country
# MAGIC     age INT -- Input parameter for age
# MAGIC )
# MAGIC RETURNS VOID -- Procedure returns no value
# MAGIC LANGUAGE SQL
# MAGIC AS 
# MAGIC $$
# MAGIC     -- First query: Fetch employee details based on age and country
# MAGIC     SELECT emp_name, emp_age, emp_sal 
# MAGIC     FROM df_table
# MAGIC     WHERE emp_age = age AND country_name = country;
# MAGIC
# MAGIC     -- Second query: Count employees with age greater than the input parameter
# MAGIC     SELECT COUNT(emp_name) AS employee_count 
# MAGIC     FROM df_table
# MAGIC     WHERE emp_age > age;
# MAGIC $$;
# MAGIC
