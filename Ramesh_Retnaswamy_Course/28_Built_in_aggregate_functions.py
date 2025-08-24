# Databricks notebook source
# MAGIC %md
# MAGIC #Aggregate Functions:- 
# MAGIC Aggregate functions are functions which operate on a group of rows and return a single value
# MAGIC
# MAGIC Aggregate functions mostly perform mathematical operations
# MAGIC
# MAGIC Some of Aggregate functions are :
# MAGIC
# MAGIC
# MAGIC 1)sum
# MAGIC 2)avg
# MAGIC 3)count
# MAGIC 4)max
# MAGIC 5)min
# MAGIC 6)median
# MAGIC 7)mode
# MAGIC 8)collect_list
# MAGIC 9)collect_set
# MAGIC
# MAGIC #Syntax:
# MAGIC
# MAGIC df_1=df.agg(sum("col_name"))
# MAGIC  
# MAGIC          or
# MAGIC
# MAGIC df_1=df.select(sum("col_name"))
# MAGIC
# MAGIC We can  use both "agg"  aggregate keyword or select keyword

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final",header=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import sum ,count, countDistinct,avg,max,min,median,mode,collect_list,collect_set

# COMMAND ----------

#sum
# Sum is used to return sum of values of a specified column 
demo_df=df.agg(sum("latitude").alias("latitude_sum"))
#display(demo_df)


# count is used to return number of rows  of a specified column
demo_df1=df.agg(count("circuit_id").alias("circuit_id_count"))
display(demo_df1)

# avg is used to return average value  of a specified column
demo_df2=df.agg(avg("latitude").alias("lat_avg"))
display(demo_df2)

#max is used to return maximum value or highest value of a specified column
demo_df3=df.agg(max("latitude").alias("latitude_highest"))
display(demo_df3)

#min is used to return minimum or lowest value of a specified column
demo_df4=df.agg(min("latitude").alias("latitude_lowest"))
display(demo_df4)

# COMMAND ----------

#mode is used to return the most frequent value in the specified column 
demo_df5=df.agg(mode("latitude").alias("latitude_mode"))
display(demo_df5)


#median is used to return the middle value of the column when column is arranged ascending order in the specified column
demo_df6=df.agg(median("latitude").alias("latitude_median"))
display(demo_df6)

#collect_list is used to collect all the values of a specified column in the form of a  list,It includes duplicates
demo_df7=df.agg(collect_list("country").alias("list_country"))
display(demo_df7)

#collect_set is used to collect all the values of a specified column in the form of a list ,It doesn't include duplicates
demo_df8=df.agg(collect_set("country").alias("country_values_with_no_duplicates"))
display(demo_df8)

# COMMAND ----------

#count distinct is used to count the no. of uniques rows in a specified column
demo_df9=df.select(countDistinct("country").alias("no._of_countries"))
display(demo_df9)

# COMMAND ----------

#we can also use multiple aggregate functions in a single code 

demo_df10=df.select(sum("latitude").alias("latitude_sum"),countDistinct("country").alias("unique_countries"),count("country").alias("countries_count"),collect_list("country").alias("countries_list"))
display(demo_df10)
