# Databricks notebook source
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,BooleanType,DateType
# from pyspark.sql

# COMMAND ----------

schema=StructType(fields=[StructField("name",StringType(),nullable=True),
                           StructField("age",IntegerType(),nullable=True),
                           StructField("Sex",StringType(),nullable=True)
                           ])

# COMMAND ----------

data=[("Shilpa",24,"Male"),
      ("Kishore",24,"Female"),
      ("Swathi",24,"Male"),
      ("Ajay",24,"Female")]

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df 
# MAGIC
# MAGIC --Here u can see the gender is incorrect
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC If you try to update or replace the Sex column directly one value at a time (e.g., first change 'Male' to 'Female', then 'Female' to 'Male'), you'll lose the original values. For example:
# MAGIC
# MAGIC 'Male' → 'Female' ✅
# MAGIC
# MAGIC Then 'Female' → 'Male' ❌ (this includes the ones you just changed!)
# MAGIC
# MAGIC This creates incorrect results because the changes overwrite each other. Using CASE avoids this by evaluating conditions on the original data before any changes are made.
# MAGIC
# MAGIC
# MAGIC ### Query:
# MAGIC ```sql
# MAGIC %sql
# MAGIC SELECT *,
# MAGIC        CASE 
# MAGIC            WHEN Sex = 'Male' THEN 'Female'
# MAGIC            WHEN Sex = 'Female' THEN 'Male'
# MAGIC        END AS sex
# MAGIC FROM df
# MAGIC ```
# MAGIC
# MAGIC ### Explanation:
# MAGIC - You're using a **`CASE` statement** to **flip the gender** value in the `Sex` column.
# MAGIC - If the value is `'Male'`, it becomes `'Female'`, and vice versa.
# MAGIC - The result is shown in a new column named `correct_sex`.
# MAGIC
# MAGIC **Example:**
# MAGIC
# MAGIC | Name  | Sex    | sex    |
# MAGIC |-------|--------|--------|
# MAGIC | John  | Male   | Female |
# MAGIC | Alice | Female | Male   |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,case when Sex='Male' then 'Female'
# MAGIC          when Sex='Female' then 'Male'
# MAGIC     end as correct_sex
# MAGIC     from df
