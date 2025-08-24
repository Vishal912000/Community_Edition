# Databricks notebook source
# MAGIC %md
# MAGIC #Window functions :-
# MAGIC Window functions are a group of functions that allow you to perform calculations across a set of rows related to current row
# MAGIC
# MAGIC Unlike regular aggregate functions like SUM (or) Avg, Window functions do not collapse into a single row but provide a result for each row in a result set  
# MAGIC
# MAGIC Types of window fumctions are 
# MAGIC
# MAGIC
# MAGIC 1.Rank()
# MAGIC
# MAGIC 2.Dense_Rank()
# MAGIC
# MAGIC 3.Row_number()
# MAGIC
# MAGIC 4.Lag()
# MAGIC
# MAGIC 5.Lead()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,sum,countDistinct,count,dense_rank,row_number,lag,lead

# COMMAND ----------

races_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result")

# COMMAND ----------

#display(races_df)

# COMMAND ----------

races_filter=races_df.filter("race_year in (2019,2020)")
display(races_filter)

# COMMAND ----------

races_group=races_filter.groupBy("driver_name")
display(races_group)

# COMMAND ----------

# Here we are grouping the data  first with race_year and then with driver_name and making a aggregartion of sum(points) and number_of races 
races_group=races_filter.groupBy("race_year","driver_name")\
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))
display(races_group)

# COMMAND ----------

# To use window functions we need to define window specification ,Specification is nothing but what columns we are using  in partitionby and orderby functions 
window_spec=Window.partitionBy("race_year").orderBy(races_group.total_points.desc())

# COMMAND ----------

# MAGIC %md #Rank():

# COMMAND ----------

#Rank:-Rank() is used to assign a rank to each row within a partition with gaps in rank values if there are ties

#If two rows have same rank then it is a tie,so when a tie happens the next rank is skipped

#Eg:-1,2,2,2,5 (So, here we can see the 3 people's rank  got tied with 2 rank, so it skipped the 3 and 4 and given the rank 5  )

#Syntax: df_rank=df.withColumn("new_col_name",rank().over(window_specification_name))

df_rank=races_group.withColumn("rank",rank().over(window_spec))
display(df_rank)

# COMMAND ----------

# MAGIC %md #dense_rank()

# COMMAND ----------

# Dense_Rank(): 
# Dense_rank () is used to assign a rank to each row within a partition without gaps in rank values if there are ties 

#Eg:-1,2,2,2,3,4 (So here we can see there are 3 prople with 2 rank ,After giving rank of 2 then it gives 3   )

df_dense_rank=races_group.withColumn("actual_rank",dense_rank().over(window_spec))
display(df_dense_rank)


# COMMAND ----------

#Let us take example of Olympics ,If there are countries which are having same number of medals

# USA      -25
# India    -20
# Russia   -10
# China    -10
# UK       -10
# CANADA   - 5

# So if we take a rank of these then it would be 1,2,3,3,3,6
# So if we take a rank of these then it would be 1,2,3,3,3,4

#oka vela muggurki same score oste rank and dense rank aa mugguriki 1 rank istay
#difference ekkada vastundi ante aa mugguri tarvatha ante 4th person ki rank 4th rank istundi ,but adhe dense_rank 2nd rank istundi

# COMMAND ----------

# MAGIC %md #row_number()

# COMMAND ----------

#row_number():row_number() is used to assign a row_number within a partition  
df_row_number=races_group.withColumn("row_number",row_number().over(window_spec))
display(df_row_number)

# COMMAND ----------

# MAGIC %md
# MAGIC #lag()

# COMMAND ----------

#lag(): lag() is used to retrieve the value from a previous row in the same partition

#It is often used in time-series analysis or when you need to compare a row with its preceeding row 

#If there is no  previous row ,lag return null by default 

#we can also set how many precceding to consider and also the default value instead of a null value

#syntax:-
# df_lag=df.withColumn("new_col",lag("old_col_name").over(window_spec))


#Here ,it gives the lag of the column with one precceding value and default value is null if no previous row
df_lag=races_group.withColumn("lag",lag("total_points").over(window_spec))
display(df_lag)


#Here we are setting the preceeding value as 2 value and default value is zero
df_lag_2=races_group.withColumn("lag_2",lag("total_points",2,0).over(window_spec))
display(df_lag_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #lead():
# MAGIC

# COMMAND ----------

#lead() is used to retrieve a value from the next row in the same partition

#If there is no next row then,lead returns null by default

#syntax:-
#df_lead=df.withColumn("col_name",lead("old_col_name").over(window_spec))

#Here ,it gives the lead of the column with one succedding value and default value is null if no next row
df_lead=races_group.withColumn("lead",lead("total_points").over(window_spec))
display(df_lead)

#Here we are setting the succedding value as 2 value and default value is zero
df_lead_2=races_group.withColumn("lead_2",lead("total_points",2,0).over(window_spec))
display(df_lead_2)