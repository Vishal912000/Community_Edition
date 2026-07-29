# Databricks notebook source
# MAGIC %md
# MAGIC #coalesce():
# MAGIC coalesce is used to handle the null values ,
# MAGIC
# MAGIC coalesce takes multiple arguments and returns the 1st non-null value as a result
# MAGIC
# MAGIC
# MAGIC If all the arguments are null,coalesce will return null
# MAGIC
# MAGIC We can also give any default value 
# MAGIC
# MAGIC
# MAGIC
# MAGIC #syntax:
# MAGIC coalesce(col_1,col_2,col_3)
# MAGIC
# MAGIC #ex:
# MAGIC select coalesce(col_1,col_2,col_3) from df
# MAGIC
# MAGIC Here, It will check if there is a non-null value in the col_1
# MAGIC if it is available ,then it will return the same,
# MAGIC
# MAGIC
# MAGIC else it will move to the col_2 and check for non-null value 
# MAGIC If it is available ,then it will return the same
# MAGIC
# MAGIC Like this,It will go for all the columns
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


df=spark.sql(f"""select * from sample_table_4""")


# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df_with=df.withColumn("null_values",lit(None))
display(df_with)
df_with.createOrReplaceTempView("df_with")

#Here I am importing null values using lit and None

# COMMAND ----------

df_coalesce_1=spark.sql(f"""select coalesce(null_values,d_o_b) from df_with""" )
display(df_coalesce_1)

#here i am passing two cols one is with null and other with data ,so we can see easily how the coalesce works

# COMMAND ----------

df_coalesce_2=spark.sql(f"""select coalesce(number,age) from df_with""" )
display(df_coalesce_2)
#here we are using noth non-null columns

# COMMAND ----------

df_coalesce_3=spark.sql(f"""select coalesce(null_values,'not a null') from df_with""" )
display(df_coalesce_3)

#here we are providing not a null as default to the df_with
