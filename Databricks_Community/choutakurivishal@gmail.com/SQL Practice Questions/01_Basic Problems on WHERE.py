# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run /Workspace/Repos/choutakurivishal@gmail.com/Community_Edition/Databricks_Community/choutakurivishal@gmail.com/Tables/Employee_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_df

# COMMAND ----------

# MAGIC %md
# MAGIC **Problems on WHERE**

# COMMAND ----------

# MAGIC %sql
# MAGIC --Problem 1:-Employees with Salary Greater Than 80,000
# MAGIC select * from employee_df where salary>80000
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Problem 2:
# MAGIC --Employees who are either in the ‘HR’ department OR have more than 8 years of experience
# MAGIC
# MAGIC select * 
# MAGIC from employee_df
# MAGIC where dept='HR' or  experience_years>8

# COMMAND ----------

# MAGIC %sql
# MAGIC --Problem 3:
# MAGIC --Please give Employees Aged 25, 30 or 35
# MAGIC select * from employee_df 
# MAGIC where age in (25,30,35)
