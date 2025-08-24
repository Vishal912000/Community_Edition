# Databricks notebook source
# MAGIC %md 
# MAGIC #split(): 
# MAGIC split function is used to split a string or column into a list  or array using the separator 
# MAGIC
# MAGIC The separator or delimeter is chosen depending on pattern of data in the column 
# MAGIC
# MAGIC so, if there is data like Vishal_choutakuri
# MAGIC we need to use separator as "_" 
# MAGIC
# MAGIC If Vishal/Choutakuri then "/"
# MAGIC
# MAGIC So we are taking the column data reference and giving the separator 
# MAGIC
# MAGIC
# MAGIC #syntax :
# MAGIC
# MAGIC select *, split(col_name,separator) from table_name 
# MAGIC
# MAGIC Ex:-
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.sample_table3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,split(Full_name," ") as split_name from default.sample_table3

# COMMAND ----------

df=spark.sql(f"""select *, concat(Full_name,"/",id) as details from sample_table3""")


#So here i am juust concatenating the columns just for understanding split  and creating a temp table
df.createOrReplaceTempView("df_table")
display(df)






# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,split(details," ") as first_split from df_table
# MAGIC
# MAGIC --So here we are splitting the data based on the separator as " " space or blank

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,split (details,"/") as second_split from df_table
# MAGIC
# MAGIC -- so here we are splitting based on / as separator

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * ,split(split(details," "),"/") from df_table
# MAGIC
# MAGIC -- this is a syntax error it doesn't work like this

# COMMAND ----------

# MAGIC %md
# MAGIC split_part,
# MAGIC substr,
# MAGIC substring,
# MAGIC substring_index
# MAGIC regex_replace(),
# MAGIC char_index,explode