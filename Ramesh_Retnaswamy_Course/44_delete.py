# Databricks notebook source
# MAGIC %md # Delete
# MAGIC
# MAGIC Delete command is used to delete records from the table
# MAGIC
# MAGIC #Syntax:-
# MAGIC #delete from table_name where col_name=value
# MAGIC
# MAGIC If we don't use where condition all the data from the table will be deleted 
# MAGIC ,we can use any condition in where clause 
# MAGIC
# MAGIC
# MAGIC Delete command deletes all data but will preserve the schema or structure of the table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Here we are deleting a single row 
# MAGIC delete from demo.drivers where driver_id=1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers
# MAGIC -- we can see the row is deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC --Here we will delete multiple rows ,so here we have given a condition to delete all rows which are having dob less than or same as 1977-01-01
# MAGIC -- so we are removing all people who are born on or before 1977-01-01
# MAGIC delete from demo.drivers
# MAGIC where dob<='1977-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from demo.drivers
