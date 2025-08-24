# Databricks notebook source
#%run /Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/41_all_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo

# COMMAND ----------

# MAGIC %md
# MAGIC #DML: Data manipulation language ,It is used to manipulate or modify the data in the tables 
# MAGIC There are 5 Key SQL commands in SQL:-
# MAGIC
# MAGIC 1)Insert 
# MAGIC
# MAGIC 2)Update
# MAGIC
# MAGIC 3)Merge
# MAGIC
# MAGIC 4)Delete
# MAGIC
# MAGIC 5)Select

# COMMAND ----------

# MAGIC %md #Insert:-
# MAGIC Insert command is used to add rows to the table
# MAGIC
# MAGIC #syntax:
# MAGIC insert into table_name(col_name,col2_name,col3_name)
# MAGIC values(1,2,3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC desc demo.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC --Here we are having 8 columns in the drivers table ,
# MAGIC --now we are inserting the new row 
# MAGIC
# MAGIC --so here we have left one column out of 8 columns ,that column will show as null 
# MAGIC insert into drivers(code,dob,driver_id,driver_ref,name,nationality,number)
# MAGIC values ("VIS",date('2024-01-09'),999,"choutakuri","choutakuri vishal","Indian",1)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from drivers where name='choutakuri vishal'

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Multiple Columns insert 
# MAGIC ---Here we are inserting multiple rows using a single insert statement into the table 
# MAGIC
# MAGIC insert into drivers (code,dob,driver_id,driver_ref,name,nationality,number)
# MAGIC values  ('AKH',date('2001-08-31'),9999,'choutakuri','choutakuri akhilesh','indian',2),
# MAGIC         ('NIK',date('1998-04-20'),99999,'akula','akula nikilesh','indian',3),
# MAGIC         ('VAR',date('2000-04-16'),999999,'galigama','galigama varun','indian',4),
# MAGIC         ('RAH',date('1998-11-02'),9999999,'bodigam','bodigam rahul','indian',5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from drivers
