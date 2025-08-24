# Databricks notebook source
# MAGIC %md 
# MAGIC Ingesting/Reading Csv files using a databricks
# MAGIC
# MAGIC We can read Csv files using can be done in many ways ,We will learn the most used 2 ways:-
# MAGIC
# MAGIC 1.Using spark.read.csv
# MAGIC 2.Using spark.read.format
# MAGIC
# MAGIC 1 st we will learn how to read a file and what are the isssues we are facing and fix them and then read file properly
# MAGIC

# COMMAND ----------

df_1=spark.read.csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
#display(df_1)

# Here I am importing a csv file a dbfs file location using spark read method ,However I am facing two issues 
# a)The column names are coming as c1,c2,c3 etc so this needs to be fixed 
# b)I can see all the columns data type is a string ,so we need the datatype as which is in same as csv file  

# COMMAND ----------

df_2=spark.read.option("header",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
#display(df_2)

# Here we are using .option method and in that we are defining the the header as True so, It will give us the same header                     value which is present in csv 
# So it fixes the 1 st issue now we need to fix the 2nd issue of data types                    


# COMMAND ----------

df_3=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
#display(df_3)

#Here we are using inferSchema in option ,This method automatically takes the same data types which are available in the csv file

# COMMAND ----------

# These are 3 options which we practiced above
df_4=spark.read.csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
df_5=spark.read.option("header",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
df_6=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")

#display(df_4)
#display(df_5)
#display(df_6)

# COMMAND ----------

# MAGIC %md 
# MAGIC 2nd method to read a csv file in a databricks 

# COMMAND ----------

# This method is quite easy as we are not using .option function and giving the header and schema after csv file path
df_7=spark.read.csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header =True,inferSchema=True)
#display(df_7)

# COMMAND ----------

# 3rd method

df_8=spark.read.format("csv").option("header",True).option("inferSchema",True).load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")

df_9=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)


display(df_8)
display(df_9)

# COMMAND ----------

#All 4 ways:

df_10=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
df_11=spark.read.csv("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)
df_12=spark.read.format("csv").option("header",True).option("inferSchema",True).load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv")
df_13=spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits.csv",header=True,inferSchema=True)

#display(df_10)
#display(df_11)
#display(df_12)
#display(df_13)