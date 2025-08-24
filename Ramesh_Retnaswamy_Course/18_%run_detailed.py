# Databricks notebook source
# MAGIC %md
# MAGIC % run :- 
# MAGIC
# MAGIC 1.spark lets use reuse one notebook in another notebook using the % run magic command
# MAGIC
# MAGIC
# MAGIC
# MAGIC 2.%run runs the notebook given in the command and makes everything available from that notebook in current notebook
# MAGIC
# MAGIC 3.%run command should be the  only command in cell of the  notebook otherwise it will throw an error
# MAGIC
# MAGIC

# COMMAND ----------

#Here in this page we are defining the notebook values ,this notebook will be ran on 19_run_practice

# COMMAND ----------

raw_file_name='dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com'

#display(raw_file_name)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#Here we are creating a function to add the time to the dataframe whenever this function is called  


def add_timestamp(input_df):
    # here we are creating a function with the name as add_timestamp using def keyword and parameter as input_df ,input_df will be the df when we are calling the function 
    output_df=input_df.withColumn('ingestion_time',current_timestamp())

    #inside the function we are creating a new variable which stores the result of the function after applying with column to the input df in the parameter 
    return output_df

    # return methods is used to return the function value as output



# COMMAND ----------

#So,Here we are going to use the variable name instead of the path name as beginning of the path is same for all the files  
df=spark.read.format('csv').load(f"{raw_file_name}/circuits.csv",inferSchema=True,header=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df_final=add_timestamp(df)
