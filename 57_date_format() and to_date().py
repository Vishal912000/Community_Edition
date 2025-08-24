# Databricks notebook source
# MAGIC %md
# MAGIC #date_format():
# MAGIC
# MAGIC date_format is used to modify the date into a specific new date format
# MAGIC
# MAGIC The multiple date formats are DD-MM-YYYY,MM-DD-YYYY,YYYY-MM-DD
# MAGIC
# MAGIC #syntax:
# MAGIC select date_format(col_name,new_format) from table_name

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType
from datetime import date
from pyspark.sql.functions import col,sum

# COMMAND ----------

schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                          StructField("age",IntegerType(),nullable=True),
                          StructField("sex",StringType(),nullable=True),
                          StructField("height",DoubleType(),nullable=True),
                          StructField("d_o_b",DateType(),nullable=True),
                          StructField("d_o_j",StringType(),nullable=True)
                          ])

# COMMAND ----------

sample_data=[("vishal",24,"Male",5.11,date(2000,1,9),"22-07-2020"),
             ("akhilesh",23,"Male",5.9,date(2001,8,31),"20-08-2023")
             ]

# COMMAND ----------

df=spark.createDataFrame(sample_data,schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,date_format(d_o_b,"dd-MMM-yyyy") as new_date_format from df_table

# COMMAND ----------

# MAGIC %md
# MAGIC #to_date():-
# MAGIC to_date () is used to convert a string into date format
# MAGIC
# MAGIC #syntax:
# MAGIC select to_date(col_name,"date_format") from table_name 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --while creating a schema i created a doj column as string instead of date format to convert using to_date
# MAGIC
# MAGIC select to_date(d_o_j,"dd-MM-yyyy") from df_table
# MAGIC
# MAGIC --so,when we use to_date ,output dates always come in default format(yyyy-mm-dd)
# MAGIC --after using to_date(),we need to use date_format() to change the format  
# MAGIC
# MAGIC
# MAGIC  --to_date function always outputs dates in the default format (yyyy-MM-dd). It does not allow specifying a custom output format. If you need a different format (e.g., MM/dd/yyyy), you must use date_format to format the output.

# COMMAND ----------

