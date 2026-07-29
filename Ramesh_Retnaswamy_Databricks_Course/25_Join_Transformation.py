# Databricks notebook source
# MAGIC %md
# MAGIC #Join:
# MAGIC Joins are used to combine or merge two dataframes / tables into a one dataframe
# MAGIC
# MAGIC There are multiple types of joins in databricks
# MAGIC
# MAGIC 1)Left Join or Left Outer Join
# MAGIC
# MAGIC 2)Right Join or Right Outer Join
# MAGIC
# MAGIC 3)Inner Join
# MAGIC
# MAGIC 4)Full Join or Full Outer Join or Outer Join
# MAGIC
# MAGIC 5)Semi Join
# MAGIC
# MAGIC 6)Left Anti Join or Anti Join
# MAGIC
# MAGIC 7)Semi Join
# MAGIC

# COMMAND ----------

circuits_df=spark.read.format('parquet').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final',header=True)

races_df=spark.read.format('parquet').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_fi',header=True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col

# We are filtering races dataframe because we have lots of data in races  
races_df_filter=races_df.filter(col('year')==2019).withColumnRenamed("name","race_name").withColumnRenamed("circuit_id","race_circuit_id")
display(races_df_filter)

# COMMAND ----------

#Left Join or Left Outer Join:
# Left join returns all rows from the left table and returns only matching rows from Right table based on the condition
#syntax:-df=df_1.join(df2,df_1.col_1=df_2.col2,"left join")

circuits_races_df=circuits_df.join(races_df_filter,
                                   (circuits_df.circuit_id ==races_df_filter.race_circuit_id),
                                    'left')

display(circuits_races_df)

circuits_races_df_final=circuits_races_df.select(col("name"),
                                                 col("location"),
                                                 col("circuit_id"),
                                                 col("race_id"),
                                                 col("year"))
display(circuits_races_df_final)


# COMMAND ----------

#Right Join 
#Right join is used to return all rows from right table/ datframe and only the matching rows from the left dataframe based on the condition

circuits_races_df_right=circuits_df.join(races_df_filter,
                                         circuits_df.circuit_id==races_df_filter.race_circuit_id,
                                         "right")
display(circuits_races_df_right)

# COMMAND ----------

#Inner  Join :-Inner join returns only the matching rows from both tables/dataframes based on condition

circuits_races_inner=circuits_df.join(races_df_filter,races_df_filter.race_circuit_id==circuits_df.circuit_id,"inner")
display(circuits_races_inner)

# COMMAND ----------

#Semi Join : Semi join returns only matching rows from the left  table / dataframe based on  the condition given

#You can compare the inner join and semi join ,so, semi join returned only the columns from left table ,You can also use left semi
circuits_races_semi=circuits_df.join(races_df_filter,circuits_df.circuit_id==races_df_filter.race_circuit_id,"semi")
display(circuits_races_semi)

# COMMAND ----------

#Anti-Join :-Anti Join returns the rows from left table/dataframe which are not matching with the right table based on the condition
#It is also the reverse on semi join  
circuits_races_anti=circuits_df.join(races_df_filter,circuits_df.circuit_id==races_df_filter.race_circuit_id,"anti")
display(circuits_races_anti)

# COMMAND ----------

#Cross Join : Cross join returns the the cartesian product of both the tables/dataframes,It joins each row from left table to each row of right table
# It is like 1st row is joined to all rows of the second table,so here we are having 77 rows in circuits and 21 in races so it gives 77*21 =1617 rows
circuits_races_cross=circuits_df.crossJoin(races_df_filter)
display(circuits_races_cross)
