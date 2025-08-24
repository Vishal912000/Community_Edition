# Databricks notebook source
# MAGIC %md
# MAGIC **WHERE Clause**:
# MAGIC
# MAGIC
# MAGIC **Purpose:** Filters rows based on conditions applied to columns within in a table. 
# MAGIC
# MAGIC **Timing:** Applies before any grouping or aggregation occurs. 
# MAGIC
# MAGIC **Usage:** Used to select specific rows that meet certain criteria. 
# MAGIC
# MAGIC **Aggregate Functions:** Cannot be used directly in the WHERE clause. 
# MAGIC
# MAGIC **Example:** 
# MAGIC SELECT * FROM employees WHERE salary > 50000; (This selects all employees with a salary greater than 50,000). 

# COMMAND ----------

# MAGIC %md
# MAGIC **HAVING Clause:**
# MAGIC **Purpose:** Filters groups of rows based on conditions applied to aggregate functions (like COUNT, SUM, AVG, etc.)
# MAGIC
# MAGIC **Timing:** Applies after the data has been grouped using the GROUP BY clause. 
# MAGIC
# MAGIC **Usage:** Used to select groups that meet certain criteria based on the results of aggregate functions. 
# MAGIC
# MAGIC **Aggregate Functions:** Used to filter based on the results of aggregate functions. 
# MAGIC
# MAGIC **Example: **SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5; (This selects departments with more than 5 employees). 
# MAGIC
# MAGIC **GROUP BY Clause:** The HAVING clause is typically used inside the GROUP BY clause. 