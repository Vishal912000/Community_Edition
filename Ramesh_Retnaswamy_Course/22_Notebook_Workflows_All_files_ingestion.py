# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC #dbutilis.notebook.run("notebook_path",timeout,
# MAGIC #{parameters in dictionary}) :-
# MAGIC
# MAGIC This command is used to call/run other notebook from the current notebook
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC #dbutilis.notebook.exit("message"):-
# MAGIC
# MAGIC
# MAGIC It is used to exit from the current notebook with a message 

# COMMAND ----------

#Syntax:-dbutils.notebook.run("notebook_path",timeout,{parameters})
dbutils.notebook.run("/Users/choutakurivishal@gmail.com/Ramesh_Retnaswamy_Course/01_Accessing_a_csv_file_using_dbfs",0)

# COMMAND ----------

dbutils.notebook.exit('vishal')
