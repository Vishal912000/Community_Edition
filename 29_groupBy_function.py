# Databricks notebook source
# MAGIC %md
# MAGIC #groupBy Function :
# MAGIC groupBy is used to group data based on one or more columns given and then perform aggregate operations
# MAGIC
# MAGIC #syntax:-
# MAGIC df_group=df.groupBy("col_name")
# MAGIC
# MAGIC We need to combine group by with aggregate functions to get the result
# MAGIC
# MAGIC

# COMMAND ----------

# #3. Group by Column and Multiple Aggregations (Sum and Average)
# Let's group by department and compute both the total salary and the average salary for each department.

# Sample DataFrame:
# department	employee	salary
# HR	John	5000
# IT	Alice	8000
# HR	Bob	4000
# IT	Charlie	9000
# HR	Kate	6000
# Code:
# python
# # Group by department and calculate both sum and average of salary
# df_grouped = df.groupBy("department").agg(
#     F.sum("salary").alias("total_salary"),
#     F.avg("salary").alias("avg_salary")
# )
# df_grouped.show()
# Output:
# diff
# Copy code
# +------------+-------------+-----------+
# | department | total_salary| avg_salary|
# +------------+-------------+-----------+
# | HR         | 15000       | 5000.0    |
# | IT         | 17000       | 8500.0    |
# +------------+-------------+-----------+

# COMMAND ----------

from pyspark.sql.functions import sum,countDistinct,count

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result",header=True)
#display(df)

# COMMAND ----------

df_new=df.filter("race_year=2020")
display(df_new)

# COMMAND ----------

df_groupBy=df_new.groupBy("driver_name").agg(sum("points"),countDistinct("race_name"))
#we are grouping the data based on the driver name so,here it will give sum of all points of jack atiken in a single row ,similarly it will perform all aggregate  functions based on driver_name 

display(df_groupBy)