# Databricks notebook source
# MAGIC %md
# MAGIC #substring_index():
# MAGIC
# MAGIC substring_index extracts a substring from a given column by specifying delimeter,
# MAGIC
# MAGIC It determines how many parts of column or string to be  included based on provided count or number of occurences of the delimeter.
# MAGIC
# MAGIC #Syntax:
# MAGIC
# MAGIC substring_index(col_name,delimeter,number of occurences of delimeter or count)

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select substring_index(url,"/",3) from df_table
# MAGIC
# MAGIC --So here i am extracting the substring of url column upto third occurence of the '/' delimeter 

# COMMAND ----------

# Input String	Delimiter	Count	Output	Explanation
# 'apple,banana,orange'	','	1	"apple"	Everything up to the first comma.
# 'apple,banana,orange'	','	2	"apple,banana"	Everything up to the second comma.
# 'apple,banana,orange'	','	-1	"orange"	Everything after the last comma.
# 'apple,banana,orange'	','	-2	"banana,orange"	Everything after the second-to-last comma.
