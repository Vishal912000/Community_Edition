# Databricks notebook source
# MAGIC %run "/Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/41_all_tables_create a new extended command notebook"

# COMMAND ----------

# MAGIC %run "/Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/42_SQL_DML_Basics(Insert)"

# COMMAND ----------

#please run all tables and insert notebook if this is not working

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %md #Update:
# MAGIC
# MAGIC Update command is used to update records in a table 
# MAGIC
# MAGIC #syntax:-
# MAGIC update table_name
# MAGIC set col_name ='record_value'
# MAGIC where col2_name ='record_value'
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC --Previously while inserting i have inserted a wrong dob for my name now i am going to update that
# MAGIC
# MAGIC update drivers
# MAGIC set dob='2000-01-09'
# MAGIC where name ='choutakuri vishal'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC --Here I am updating the single row and updating the multiple columns such as nationality,driver_ref,name etc 
# MAGIC update demo.drivers 
# MAGIC set nationality ='Indian',
# MAGIC driver_ref='Kohli',
# MAGIC name='Virat Kohli',
# MAGIC dob='1978-04-08'
# MAGIC where driver_id=1
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC --Here I am updating multiple row records of a single column 
# MAGIC --So this code will update nationality column where driver_id is 1,2,3 and 4
# MAGIC update demo.drivers 
# MAGIC set nationality='Indian'
# MAGIC where driver_id in (1,2,3,4)