# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import lit

# COMMAND ----------

schema=StructType(fields=[StructField("emp_id",IntegerType(),nullable=True),
                          StructField("salary_component_type",StringType(),nullable=True),
                          StructField("value",IntegerType(),nullable=True)])

# COMMAND ----------

data=[(1,"salary",100),(1,"bonus",5),(1,"hike_percent",10),
      (2,"salary",150),(2,"bonus",8),(2,"hike_percent",15),
      (3,"salary",200),(3,"bonus",12),(3,"hike_percent",20)]

# COMMAND ----------

df=spark.createDataFrame(data,schema)
display(df)
df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %md
# MAGIC Of course! Here's the explanation in **very easy and simple English**:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **What this SQL query does:**
# MAGIC
# MAGIC 1. **It looks at each employee (`emp_id`).**
# MAGIC
# MAGIC 2. **It checks the type of money** (salary, bonus, or hike_percent) using `CASE WHEN`.
# MAGIC    
# MAGIC 3. **It adds up (`SUM`) the values (`val`)** for each type:
# MAGIC    - If the type is `'salary'`, it adds all salary values.
# MAGIC    - If the type is `'bonus'`, it adds all bonus values.
# MAGIC    - If the type is `'hike_percent'`, it adds all hike percent values.
# MAGIC
# MAGIC 4. **It shows one final row for each employee** with:
# MAGIC    - Salary total
# MAGIC    - Bonus total
# MAGIC    - Hike percent total
# MAGIC
# MAGIC 5. **It uses `GROUP BY emp_id`** to make sure each employee comes only once in the final result.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Simple Example:**
# MAGIC
# MAGIC Suppose your data is:
# MAGIC
# MAGIC | emp_id | salary_component_type | val  |
# MAGIC |--------|------------------------|------|
# MAGIC | 1      | salary                 | 10000 |
# MAGIC | 1      | bonus                  | 5000  |
# MAGIC | 1      | hike_percent           | 10    |
# MAGIC | 1      | bonus                  | 2000  |
# MAGIC
# MAGIC Here, **employee 1** has:
# MAGIC - Salary: 10000
# MAGIC - Bonus: 5000 + 2000 = **7000** (because `SUM`)
# MAGIC - Hike percent: 10
# MAGIC
# MAGIC ✅ After running the query, the output will be:
# MAGIC
# MAGIC | emp_id | salary | bonus | hike_percent |
# MAGIC |--------|--------|-------|--------------|
# MAGIC | 1      | 10000  | 7000  | 10           |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **In one line:**  
# MAGIC 👉 This query **turns rows into columns** and **adds up values** for each employee.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Would you also like me to show this with a super easy "visual table flow" next? (It’s very helpful for full understanding!) 🎯

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id,sum(case when salary_component_type ='salary' then value end) as salary,
# MAGIC               sum(case when salary_component_type='bonus' then value end )as bonus,
# MAGIC               sum(case when salary_component_type='hike_percent' then value end) as hike_percent
# MAGIC               from df
# MAGIC               group by emp_id
